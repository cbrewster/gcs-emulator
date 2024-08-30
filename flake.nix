{
  description = "GCS emulator";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:

    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      rec {
        formatter = pkgs.nixfmt-rfc-style;

        devShell = pkgs.mkShellNoCC {
          name = "go";

          buildInputs = with pkgs; [
            go
            gopls
            gotools
          ];

        };

        packages.gcs-emulator = pkgs.buildGoModule {
          pname = "gcs-emulator";
          version = "0.0.1";
          src = ./.;
          vendorHash = "sha256-ddkm24y+YP5AKd3P8JrHESl8zUSnJtefK0oSvOab2Ag=";
          doCheck = false;
        };

        checks.gcs-emulator = packages.gcs-emulator.overrideAttrs (final: {
          doCheck = true;
        });

        defaultPackage = packages.gcs-emulator;
      }
    );
}
