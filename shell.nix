{ pkgs ? import <nixpkgs> {}}:

pkgs.mkShell {
  packages = [
    pkgs.go
    pkgs.goreleaser
    pkgs.gopls
  ];
}
