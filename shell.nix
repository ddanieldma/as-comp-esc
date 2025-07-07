{ pkgs ? import <nixpkgs> {} }:

(pkgs.buildFHSUserEnv {
  name = "python-dev";
  targetPkgs = pkgs: (with pkgs; [
    python3
    uv
    # Java (required for Spark)
    openjdk11
    # Apache Spark
    spark
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
    
    # Java configuration for Spark
    export JAVA_HOME=${pkgs.openjdk11}
    
    # Spark configuration
    export SPARK_HOME=${pkgs.spark}
    export PATH=$SPARK_HOME/bin:$PATH
    
    # PySpark configuration
    export PYSPARK_PYTHON=python3
    export PYSPARK_DRIVER_PYTHON=python3
    
    # Optional: Set Spark to use Python 3 explicitly
    export SPARK_YARN_USER_ENV="PYSPARK_PYTHON=python3"
  '';
}).env