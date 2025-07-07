{ pkgs ? import <nixpkgs> {} }:

(pkgs.buildFHSUserEnv {
  name = "python-dev";
  targetPkgs = pkgs: (with pkgs; [
    python3
    uv
    # C/C++ toolchain
    gcc
    stdenv.cc.cc.lib
    # Common libraries that Python packages need
    zlib
    libffi
    openssl
    bzip2
    ncurses
    readline
    sqlite
    tk
    xz
    # Docker and container tools
    docker
    docker-compose
    # Monitoring and process management
    htop
    procps
    # Network utilities
    netcat
    curl
  ]);
  runScript = "bash";
  profile = ''
    export PYTHONNOUSERSITE=1
    unset PYTHONPATH
  '';
}).env