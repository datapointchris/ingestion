package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/datapointchris/goclilogin"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"
)

// product names the config file's directory, the state directory and the
// client id prefixes.
const product = "ingestion"

// keyringService namespaces the CLI's entries in the OS keychain. It is a
// deployed identifier, so it keeps its spelling whatever the binary is named.
const keyringService = "ingestion-cli"

// clientSecretEnv selects the client-credentials grant for the client the
// config file names, as a worker runs. It is read from the environment alone,
// so it is never written to the file and no command prints it.
const clientSecretEnv = "INGESTION_CLIENT_SECRET"

// workerScope is what the worker's service client asks for.
const workerScope = "ingestion.tasks.claim"

// deliverSecretEnv is the variable that holds the secret of the service
// client that the [deliver.<name>] table names, such as
// INGESTION_DELIVER_LIFE_ARTICLES_CLIENT_SECRET for life-articles. It is read
// from the environment alone, as clientSecretEnv is.
func deliverSecretEnv(name string) string {
	return "INGESTION_DELIVER_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_CLIENT_SECRET"
}

// sourceSecretEnv is the variable that holds the secret of the service client
// that the [source.<name>] table names, such as
// INGESTION_SOURCE_YPL_CLIENT_SECRET for ypl. It is read from the environment
// alone, as clientSecretEnv is.
func sourceSecretEnv(name string) string {
	return "INGESTION_SOURCE_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_CLIENT_SECRET"
}

// fileConfig is the config file. Its top-level api, issuer and client_id are
// goclilogin's, read by every command that calls the service. serve reads
// issuer and its own table, and worker run reads its own table.
type fileConfig struct {
	Issuer string `toml:"issuer"`
	Serve  struct {
		Listen         string `toml:"listen"`
		StateDir       string `toml:"state_dir"`
		PublicURL      string `toml:"public_url"`
		RecordsOTLPURL string `toml:"records_otlp_url"`
		// Homes maps each named root to the directory it names, where a file
		// already lives and is referenced rather than copied.
		Homes map[string]string `toml:"homes"`
	} `toml:"serve"`
	Worker struct {
		Slots int `toml:"slots"`
		// SlotLimits caps how many tasks of each slot group or ungrouped
		// capability it names run at once, within Slots.
		SlotLimits map[string]int `toml:"slot_limits"`
		Lease      duration       `toml:"lease"`
		Poll       duration       `toml:"poll"`
	} `toml:"worker"`
	// Deliver holds a table for each deliverer that a worker opens, under the
	// name the deliverer registers. worker run reads each, and serve reads
	// each one's root.
	Deliver map[string]deliverTable `toml:"deliver"`
	// Sources holds a table for each source that a worker reaches, under the
	// source's name, for the poller and the deleter that the source registers.
	Sources map[string]serviceClient `toml:"source"`
	// Model names the adapter that carries every model call, its settings,
	// and what each model stage asks its run for, read by worker run.
	Model modelConfig `toml:"model"`
	// Gates holds what serve schedules for the gates, such as when it drafts
	// a change to the quality gate's prompt from its misses.
	Gates struct {
		Proposals struct {
			// Schedule is a cron expression, as a connector's schedule reads it.
			Schedule string `toml:"schedule"`
		} `toml:"proposals"`
	} `toml:"gates"`
}

// modelConfig is the [model] table.
type modelConfig struct {
	// Adapter is "queue" or "claude-cli". With none, a worker runs no model
	// stage.
	Adapter string `toml:"adapter"`
	Queue   struct {
		serviceClient
		// Agent is the kind of run the dispatch service starts.
		Agent string `toml:"agent"`
	} `toml:"queue"`
	ClaudeCLI struct {
		// Command is the claude binary, by path or by name on PATH.
		Command string `toml:"command"`
	} `toml:"claude-cli"`
	Contents       stageModel `toml:"contents"`
	Quality        stageModel `toml:"quality"`
	ModelRead      stageModel `toml:"model-read"`
	PromptProposal stageModel `toml:"prompt-proposal"`
}

// stageModel is what one model stage asks its run for.
type stageModel struct {
	Model  string `toml:"model"`
	Effort string `toml:"effort"`
}

// modelSecretEnv holds the secret of the service client that [model.queue]
// names, read from the environment alone, as clientSecretEnv is.
const modelSecretEnv = "INGESTION_MODEL_QUEUE_CLIENT_SECRET"

// serviceClient is how a worker reaches a destination app: the app's API, and
// the service client it writes as, by client_id or by the token that
// token_command prints. An empty issuer takes the file's.
type serviceClient struct {
	API          string   `toml:"api"`
	ClientID     string   `toml:"client_id"`
	TokenCommand []string `toml:"token_command"`
	Issuer       string   `toml:"issuer"`
}

// deliverTable is one [deliver.<name>] table: how a worker reaches the app,
// and, for a deliverer that writes an item by its file, the [serve.homes]
// root below which serve reads each item's path.
type deliverTable struct {
	serviceClient
	Root string `toml:"root"`
}

// duration is a TOML string such as "5m".
type duration struct{ time.Duration }

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// configPath is the --config flag's value where cmd declares one and it is
// set, and $XDG_CONFIG_HOME/ingestion/config.toml otherwise.
func configPath(cmd *cobra.Command) string {
	if f := cmd.Flags().Lookup("config"); f != nil && f.Value.String() != "" {
		return f.Value.String()
	}
	return goclilogin.ConfigPath(product)
}

// addConfigFlag declares --config on cmd.
func addConfigFlag(cmd *cobra.Command) {
	cmd.Flags().String("config", "", "Read this config file (default $XDG_CONFIG_HOME/ingestion/config.toml)")
}

// readConfig reads the config file at path. A file that is missing reads as
// every default.
func readConfig(path string) (fileConfig, error) {
	var c fileConfig
	_, err := toml.DecodeFile(path, &c)
	if errors.Is(err, fs.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, fmt.Errorf("read %s: %w", path, err)
	}
	return c, nil
}

// stateDir is where serve keeps its store: the file's state_dir, or
// $XDG_STATE_HOME/ingestion.
func stateDir(c fileConfig) string {
	if c.Serve.StateDir != "" {
		return c.Serve.StateDir
	}
	if dir := os.Getenv("XDG_STATE_HOME"); dir != "" {
		return filepath.Join(dir, product)
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "state", product)
}

// errNeedsLogin is a command run where this machine holds no token.
var errNeedsLogin = errors.New("not logged in — run `ingestion auth login`")

// loadDeployment reads where the CLI is pointed from the config file at path,
// with api outranking the file's.
func loadDeployment(ctx context.Context, path, api string) (goclilogin.Deployment, error) {
	return goclilogin.LoadDeploymentFrom(ctx, product, path, api)
}

// loginConfig is the goclilogin view of a deployment for a person's login.
func loginConfig(d goclilogin.Deployment) goclilogin.Config {
	return goclilogin.Config{
		Issuer: d.Issuer.Value, ClientID: d.ClientID.Value,
		KeyringService: keyringService, StateDir: goclilogin.StateDir(product),
	}
}

// tokenSource is the file's token_command where it names one, the
// client-credentials grant where INGESTION_CLIENT_SECRET is set, and the token
// this machine logged in for otherwise.
func tokenSource(ctx context.Context, d goclilogin.Deployment) (oauth2.TokenSource, error) {
	if d.HandsTokens() {
		return goclilogin.CommandTokenSource(ctx, d.TokenCommand.Argv())
	}
	if secret := os.Getenv(clientSecretEnv); secret != "" {
		return goclilogin.ClientCredentialsTokenSource(ctx,
			goclilogin.ServiceClient{Issuer: d.Issuer.Value, ClientID: d.ClientID.Value, Scopes: []string{workerScope}}, secret)
	}
	login := loginConfig(d)
	source, err := goclilogin.TokenSource(ctx, login, goclilogin.NewTokenStore(login))
	if errors.Is(err, goclilogin.ErrNotLoggedIn) {
		return nil, errNeedsLogin
	}
	return source, err
}
