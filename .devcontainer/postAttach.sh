#!/bin/sh

cd `dirname $0`
cd ..
sudo chown -R vscode . /home/vscode/go

# install go development kit
go install golang.org/x/tools/gopls@latest
go install honnef.co/go/tools/cmd/staticcheck@latest
go install github.com/go-delve/delve/cmd/dlv@latest

go mod download

sudo chmod 777 /home/vscode/.aws/cli
