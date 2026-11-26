# syntax=docker/dockerfile:1

# One image runs both processes: `ingestion serve` and `ingestion worker run`.
# The Go stage's tag is the toolchain go.mod names.
FROM golang:1.26.11 AS build
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download
# The release names the version, commit and commit date it builds. A build
# that names none of them reports a dev build.
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/datapointchris/ingestion/cmd.version=${VERSION} -X github.com/datapointchris/ingestion/cmd.commit=${COMMIT} -X github.com/datapointchris/ingestion/cmd.date=${DATE}" \
      -o /out/ingestion .

# The scan's rules: Agent Threat Rules at the commit download-atr-rules pins,
# converted by build-rules with the yr that install-tools pins, beside
# ingestion's own. ATR is MIT licensed, so its license ships with the rules.
RUN --mount=type=bind,source=release,target=/src/release \
    release/install-tools /usr/local/bin yr \
    && release/download-atr-rules /atr
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    go build -o /usr/local/bin/build-rules ./scan/build-rules \
    && build-rules --atr /atr --yr /usr/local/bin/yr --out /out/scan-rules \
    && cp /atr/LICENSE /out/scan-rules/ATR-LICENSE

# pdf-docling's layout and table models, each file at the commit and SHA-256
# that download-docling-models pins. Their stage reads only that script, so a
# source change leaves the download cached.
FROM golang:1.26.11 AS docling-models
RUN --mount=type=bind,source=release/download-docling-models,target=/usr/local/bin/download-docling-models \
    download-docling-models /out/docling-models

# audio-whisper's model, each file at the commit and SHA-256 that
# download-whisper-model pins. Its stage reads only that script, as the
# Docling models' does.
FROM golang:1.26.11 AS whisper-model
RUN --mount=type=bind,source=release/download-whisper-model,target=/usr/local/bin/download-whisper-model \
    download-whisper-model /out/whisper-models

# The ffmpeg that audio-whisper decodes audio with, at the build and SHA-256
# that install-tools pins, with its LGPL license. Its archive is xz, which the
# Go image cannot unpack, so the stage installs xz first.
FROM golang:1.26.11 AS ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends xz-utils && rm -rf /var/lib/apt/lists/*
RUN --mount=type=bind,source=release/install-tools,target=/usr/local/bin/install-tools \
    install-tools /out/ffmpeg ffmpeg

# Debian rather than a static base, because the worker runs extractors whose
# libraries are Python as programs in this image.
FROM debian:trixie-slim

LABEL org.opencontainers.image.source="https://github.com/datapointchris/ingestion" \
      org.opencontainers.image.description="ingestion: lands each item from an outside source once, and delivers it to the one place it belongs" \
      org.opencontainers.image.licenses="MIT AND AGPL-3.0-or-later"

# A uid of its own, so a file it writes to a mounted volume reads as
# ingestion's. The store and the blobs live on a volume mounted at
# /var/lib/ingestion.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini python3 python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10600 ingestion \
    && useradd --system --uid 10600 --gid ingestion --no-create-home --shell /usr/sbin/nologin ingestion \
    && install -d -o ingestion -g ingestion /var/lib/ingestion /etc/ingestion

# The extractor programs' Python libraries, each at the version and hashes
# release/python-requirements.txt pins, in the venv their shebangs name.
# PyMuPDF is AGPL-3.0, so its notice and the license's text ship beside
# ingestion's own MIT license. Docling's models ship with their licenses'
# texts: Apache-2.0 for the layout model and CDLA-Permissive-2.0 for
# TableFormer. Whisper's weights ship with OpenAI's MIT notice, and ffmpeg
# with its LGPL notice.
RUN --mount=type=bind,source=release/python-requirements.txt,target=/tmp/python-requirements.txt \
    python3 -m venv /opt/ingestion/venv \
    && /opt/ingestion/venv/bin/pip install --no-cache-dir --disable-pip-version-check --require-hashes --only-binary :all: \
       -r /tmp/python-requirements.txt \
    && install -d /usr/local/share/ingestion/licenses \
    && cp /opt/ingestion/venv/lib/python3*/site-packages/pymupdf-*.dist-info/COPYING /usr/local/share/ingestion/licenses/PyMuPDF-COPYING
COPY LICENSE /usr/local/share/ingestion/licenses/MIT-LICENSE
COPY LICENSES/AGPL-3.0-or-later.txt LICENSES/Apache-2.0.txt LICENSES/CDLA-Permissive-2.0.txt LICENSES/Whisper-MIT.txt \
     /usr/local/share/ingestion/licenses/
# Where pdf-docling's ARTIFACTS names them.
COPY --from=docling-models /out/docling-models /usr/local/share/ingestion/docling-models
# Where audio-whisper's MODEL and FFMPEG name them.
COPY --from=whisper-model /out/whisper-models /usr/local/share/ingestion/whisper-models
COPY --from=ffmpeg /out/ffmpeg/ffmpeg /usr/local/bin/ffmpeg
COPY --from=ffmpeg /out/ffmpeg/ffmpeg-LICENSE.txt /usr/local/share/ingestion/licenses/FFmpeg-LICENSE.txt

# The venv, the models and ffmpeg sit above the binary, so a release that
# moves none of them keeps their layers and their digests.
COPY --from=build /out/ingestion /usr/local/bin/ingestion
# Where scan.Yr and scan.RulesDir name them.
COPY --from=build /usr/local/bin/yr /usr/local/bin/yr
COPY --from=build /out/scan-rules /usr/local/share/ingestion/scan-rules
# Where extract.ProgramDir names them.
COPY extract/programs/ /usr/local/libexec/ingestion/extractors/

# Numeric, so a pod with runAsNonRoot can verify it without reading /etc/passwd.
USER 10600:10600
ENV XDG_STATE_HOME=/var/lib \
    XDG_CONFIG_HOME=/etc
EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "ingestion"]
CMD ["serve"]
