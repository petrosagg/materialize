# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

with import <nixpkgs> {};

stdenv.mkDerivation rec {
  name = "materialize";
  buildInputs = with pkgs; [
      openssl
      zlib.static
      cmake
      pkgconfig
      rustfmt
      # rustup
      lld
      lsof # for mtrlz-shell
      curl # for testing
      confluent-platform
      shellcheck
    ];
  shellHook = ''
    # TODO(jamii) using $(pwd) is fragile
    export PATH=$(pwd)/bin/:$PATH
    export MZ_DEV=1
    ulimit -m $((8*1024*1024))
    ulimit -v $((8*1024*1024))
   '';
}
