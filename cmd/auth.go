package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/goclilogin"
	"github.com/pkg/browser"
	"github.com/spf13/cobra"
)

// loginTimeout backs up the device code's own expiry, so a login left open
// ends rather than holding the terminal.
const loginTimeout = 15 * time.Minute

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Log this machine in to the service",
	Long: "Logging in prints a code and a link. Approve it in a browser on any device,\n" +
		"so it works over SSH too.\n" +
		"\n" +
		"Where ingestion is pointed comes from one config file,\n" +
		"$XDG_CONFIG_HOME/ingestion/config.toml: its api, issuer and client_id. `ingestion\n" +
		"auth login <url>` fills in whatever it lacks, and `ingestion auth status` prints it.",
}

var authLoginCmd = &cobra.Command{
	Use:   "login [url]",
	Short: "Log in by approving a code in any browser",
	Long: "Log in to the service at url, or at the config file's api where url is\n" +
		"omitted. Whatever the file lacks is filled in: api from url, the issuer from\n" +
		"the service's RFC 9728 metadata, and the client id from --client-id or\n" +
		"ingestion-cli-<short hostname>. A value already in the file is never rewritten.",
	Example: "  ingestion auth login https://ingestion.example.com",
	Args:    cobra.MaximumNArgs(1),
	RunE:    runAuthLogin,
}

var authStatusCmd = &cobra.Command{
	Use:     "status",
	Short:   "Print where this machine is pointed, and whether its login still holds",
	Example: "  ingestion auth status",
	Args:    cobra.NoArgs,
	RunE:    runAuthStatus,
}

var authLogoutCmd = &cobra.Command{
	Use:     "logout",
	Short:   "Remove this machine's stored token",
	Example: "  ingestion auth logout",
	Args:    cobra.NoArgs,
	RunE:    runAuthLogout,
}

func init() {
	goclikit.AsNamespace(authCmd)
	authLoginCmd.Flags().String("client-id", "", "Log in as this client where the config file names none (default ingestion-cli-<short hostname>)")
	for _, c := range []*cobra.Command{authLoginCmd, authStatusCmd, authLogoutCmd} {
		addConfigFlag(c)
	}
	authCmd.AddCommand(authLoginCmd, authStatusCmd, authLogoutCmd)
	rootCmd.AddCommand(authCmd)
}

func runAuthLogin(cmd *cobra.Command, args []string) error {
	if os.Getenv(clientSecretEnv) != "" {
		return fmt.Errorf("%s is set, so ingestion authenticates as a service client with no login; unset it to log in as a person", clientSecretEnv)
	}
	url := ""
	if len(args) == 1 {
		url = args[0]
	}
	clientID, _ := cmd.Flags().GetString("client-id")
	ctx, cancel := context.WithTimeout(cmd.Context(), loginTimeout)
	defer cancel()
	d, err := goclilogin.DeploymentForLoginFrom(ctx, product, configPath(cmd), url, clientID)
	if err != nil {
		return err
	}
	stderr := cmd.ErrOrStderr()
	_, _ = fmt.Fprintln(stderr, "Logging in to:")
	goclilogin.WriteDeployment(stderr, d)
	_, _ = fmt.Fprintln(stderr)
	login := loginConfig(d)
	token, err := goclilogin.Login(ctx, login, func(p goclilogin.DevicePrompt) {
		goclilogin.WriteInstructions(stderr, login.ClientID, p)
		if quiet, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0); err == nil {
			browser.Stdout, browser.Stderr = quiet, quiet
		}
		_ = browser.OpenURL(p.BrowserURL())
	})
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("nothing approved the code within %s; run `ingestion auth login` again", loginTimeout)
	}
	if err != nil {
		return err
	}
	tokens := goclilogin.NewTokenStore(login)
	backend, err := tokens.Save(login.ClientID, token)
	if err != nil {
		return fmt.Errorf("save the token: %w", err)
	}
	if backend == goclilogin.BackendFile {
		_, _ = fmt.Fprintf(stderr, "\nThis machine has no OS keyring, so the token is in %s, readable by this user alone.\n", tokens.FilePath())
	}
	if err := d.Save(); err != nil {
		return fmt.Errorf("logged in as %s, but the config file was not written: %w", login.ClientID, err)
	}
	_, _ = fmt.Fprintf(stderr, "\nLogged in as %s\n", login.ClientID)
	return nil
}

func runAuthStatus(cmd *cobra.Command, _ []string) error {
	d, err := loadDeployment(cmd.Context(), configPath(cmd), "")
	out := cmd.OutOrStdout()
	goclilogin.WriteDeployment(out, d)
	if err != nil {
		return err
	}
	if d.HandsTokens() {
		_, _ = fmt.Fprintln(out, "\ntoken_command hands this machine its tokens.")
		return nil
	}
	login := loginConfig(d)
	state, _ := goclilogin.VerifySession(cmd.Context(), login, goclilogin.NewTokenStore(login))
	_, _ = fmt.Fprintf(out, "\nsession: %s\n", state)
	if state != goclilogin.SessionLive {
		return errors.New("this machine holds no live session; run `ingestion auth login`")
	}
	return nil
}

func runAuthLogout(cmd *cobra.Command, _ []string) error {
	d, err := loadDeployment(cmd.Context(), configPath(cmd), "")
	if err != nil && !errors.Is(err, goclilogin.ErrNotConfigured) {
		return err
	}
	if d.ClientID.Value == "" {
		d.ClientID.Value = goclilogin.ClientID(product)
	}
	err = goclilogin.NewTokenStore(loginConfig(d)).Delete(d.ClientID.Value)
	if errors.Is(err, goclilogin.ErrNotLoggedIn) {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Not logged in, so there is nothing to remove.")
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove the token: %w", err)
	}
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Logged out (%s).\n", d.ClientID.Value)
	return nil
}
