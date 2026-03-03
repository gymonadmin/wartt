#!/usr/bin/env bash
set -euo pipefail

MIN_GO_VERSION="${MIN_GO_VERSION:-1.24.2}"
INSTALL_ROOT="${WARTT_INSTALL_ROOT:-/opt/wartt}"
BIN_LINK="${WARTT_BIN_LINK:-/usr/local/bin/wartt}"
LOG_DIR="${WARTT_LOG_DIR:-/var/log/wa-latency}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_MANAGER=""
PKG_UPDATED=0

log() {
  printf '[wartt-install] %s\n' "$*"
}

die() {
  printf '[wartt-install] ERROR: %s\n' "$*" >&2
  exit 1
}

as_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

detect_pkg_manager() {
  if command -v apt-get >/dev/null 2>&1; then
    PKG_MANAGER="apt-get"
  elif command -v dnf >/dev/null 2>&1; then
    PKG_MANAGER="dnf"
  elif command -v yum >/dev/null 2>&1; then
    PKG_MANAGER="yum"
  elif command -v pacman >/dev/null 2>&1; then
    PKG_MANAGER="pacman"
  elif command -v zypper >/dev/null 2>&1; then
    PKG_MANAGER="zypper"
  elif command -v apk >/dev/null 2>&1; then
    PKG_MANAGER="apk"
  fi
}

pkg_install() {
  [[ -n "$PKG_MANAGER" ]] || die "no supported package manager found"
  case "$PKG_MANAGER" in
    apt-get)
      if [[ "$PKG_UPDATED" -eq 0 ]]; then
        as_root apt-get update -y
        PKG_UPDATED=1
      fi
      as_root apt-get install -y "$@"
      ;;
    dnf)
      as_root dnf install -y "$@"
      ;;
    yum)
      as_root yum install -y "$@"
      ;;
    pacman)
      as_root pacman -Sy --noconfirm --needed "$@"
      ;;
    zypper)
      as_root zypper --non-interactive install "$@"
      ;;
    apk)
      as_root apk add --no-cache "$@"
      ;;
    *)
      die "unsupported package manager: $PKG_MANAGER"
      ;;
  esac
}

version_ge() {
  local have="$1"
  local need="$2"
  [[ "$(printf '%s\n' "$need" "$have" | sort -V | head -n1)" == "$need" ]]
}

go_version_value() {
  go version | awk '{print $3}' | sed 's/^go//'
}

ensure_make() {
  if command -v make >/dev/null 2>&1; then
    return 0
  fi
  [[ -n "$PKG_MANAGER" ]] || die "make not found and no package manager detected"
  log "Installing make"
  pkg_install make
}

ensure_fetch_tools() {
  if command -v curl >/dev/null 2>&1 || command -v wget >/dev/null 2>&1; then
    :
  else
    [[ -n "$PKG_MANAGER" ]] || die "need curl or wget to install Go toolchain"
    log "Installing curl"
    pkg_install curl
  fi
  if ! command -v tar >/dev/null 2>&1; then
    [[ -n "$PKG_MANAGER" ]] || die "need tar to install Go toolchain"
    log "Installing tar"
    pkg_install tar
  fi
}

install_go_from_pkg() {
  [[ -n "$PKG_MANAGER" ]] || return 1
  log "Installing Go from package manager"
  case "$PKG_MANAGER" in
    apt-get) pkg_install golang-go ;;
    dnf|yum) pkg_install golang ;;
    pacman) pkg_install go ;;
    zypper) pkg_install go ;;
    apk) pkg_install go ;;
    *) return 1 ;;
  esac
}

install_go_tarball() {
  [[ "$(uname -s)" == "Linux" ]] || die "automatic Go tarball install is Linux-only"
  local arch
  case "$(uname -m)" in
    x86_64|amd64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *)
      die "unsupported architecture for Go tarball: $(uname -m)"
      ;;
  esac

  ensure_fetch_tools

  local url="https://go.dev/dl/go${MIN_GO_VERSION}.linux-${arch}.tar.gz"
  local tmpdir
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' EXIT
  local tgz="$tmpdir/go.tgz"

  log "Downloading Go ${MIN_GO_VERSION} from ${url}"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$tgz"
  else
    wget -qO "$tgz" "$url"
  fi

  log "Installing Go ${MIN_GO_VERSION} to /usr/local/go"
  as_root rm -rf /usr/local/go
  as_root tar -C /usr/local -xzf "$tgz"
  as_root ln -sf /usr/local/go/bin/go /usr/local/bin/go
  as_root ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt

  export PATH="/usr/local/go/bin:/usr/local/bin:$PATH"
}

ensure_go() {
  if command -v go >/dev/null 2>&1; then
    local have
    have="$(go_version_value)"
    if version_ge "$have" "$MIN_GO_VERSION"; then
      return 0
    fi
    log "Go $have is older than required $MIN_GO_VERSION"
  else
    log "Go is not installed"
  fi

  if [[ -n "$PKG_MANAGER" ]]; then
    install_go_from_pkg || true
  fi

  if command -v go >/dev/null 2>&1; then
    local have_pkg
    have_pkg="$(go_version_value)"
    if version_ge "$have_pkg" "$MIN_GO_VERSION"; then
      return 0
    fi
    log "Package manager Go ($have_pkg) is still below $MIN_GO_VERSION"
  fi

  install_go_tarball

  local have_final
  have_final="$(go_version_value)"
  version_ge "$have_final" "$MIN_GO_VERSION" || die "failed to install Go >= $MIN_GO_VERSION (have $have_final)"
}

install_layout() {
  log "Installing wartt files to ${INSTALL_ROOT}"
  as_root mkdir -p \
    "${INSTALL_ROOT}/bin" \
    "${INSTALL_ROOT}/config"

  if [[ "${SCRIPT_DIR}" -ef "${INSTALL_ROOT}" ]]; then
    log "Install root matches repository dir; skipping file copy"
  else
    as_root install -m 0755 "${SCRIPT_DIR}/bin/wartt" "${INSTALL_ROOT}/bin/wartt"
    as_root cp -a "${SCRIPT_DIR}/config/." "${INSTALL_ROOT}/config/"
    as_root install -m 0644 "${SCRIPT_DIR}/README.md" "${INSTALL_ROOT}/README.md"
  fi
  as_root ln -sf "${INSTALL_ROOT}/bin/wartt" "${BIN_LINK}"
}

prepare_runtime_dir() {
  log "Preparing runtime log dir ${LOG_DIR}"
  as_root mkdir -p "${LOG_DIR}"
  if [[ -n "${SUDO_USER:-}" ]]; then
    as_root chown "${SUDO_USER}:${SUDO_USER}" "${LOG_DIR}" || true
  fi
}

main() {
  [[ -f "${SCRIPT_DIR}/go.mod" ]] || die "run this script from the wartt repository root"

  detect_pkg_manager
  ensure_make
  ensure_go

  export PATH="/usr/local/go/bin:/usr/local/bin:$PATH"
  cd "${SCRIPT_DIR}"

  log "Building wartt"
  mkdir -p "${SCRIPT_DIR}/bin"
  go mod download
  go build -trimpath -o "${SCRIPT_DIR}/bin/wartt" ./cmd/wartt

  install_layout
  prepare_runtime_dir

  log "Install complete"
  log "Run: wartt"
}

main "$@"
