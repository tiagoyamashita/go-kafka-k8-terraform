# Setup

Install the tools this repo uses on Windows, then follow [build-and-run.md](build-and-run.md).

This machine currently reports Go 1.18.1 and Terraform 0.13.4. The repo needs Go 1.26 or newer and Terraform 1.5 or newer. `-chdir` does not exist in Terraform 0.13, which is why `terraform -chdir=terraform/cluster init` fails.

Run the installers from an Administrator terminal. After each install, close that terminal and open a new one so `PATH` picks up the new binary.

## See what is already installed

```bat
go version
terraform version
docker version
kind version
kubectl version --client
where go
where terraform
```

`where` prints every match. The first line is the one the shell runs.

## Go

The project builds with Go 1.26 or newer. Install 1.27.3:

```bat
winget install -e --id GoLang.Go --version 1.27.3
```

If winget says Go is already installed:

```bat
winget upgrade -e --id GoLang.Go
```

Open a new terminal and check:

```bat
go version
```

Expected: `go version go1.27.3 windows/amd64`, or any 1.26+ release.

## Terraform

The copy on this machine is Terraform 0.13.4 from Chocolatey (`C:\ProgramData\chocolatey\bin\terraform.exe`). Upgrade that copy so it stays first on `PATH`:

```bat
choco upgrade terraform -y
```

Open a new terminal and check:

```bat
where terraform
terraform version
```

Expected: version 1.5 or newer. If `where` still lists 0.13.4 first, that old binary is winning. Upgrade it with Chocolatey rather than installing a second copy somewhere else.

## Docker Desktop

kind runs the Kubernetes node as a Docker container. Terraform cannot create the cluster until Docker is running.

```bat
winget install -e --id Docker.DockerDesktop
```

Start Docker Desktop and wait until it reports that the engine is running. Then:

```bat
docker version
```

Both the client and the server sections should print a version.

## kubectl

```bat
winget install -e --id Kubernetes.kubectl
```

Open a new terminal and check:

```bat
kubectl version --client
```

## kind

```bat
winget install -e --id Kubernetes.kind
```

Open a new terminal and check:

```bat
kind version
```

## Confirm

Open one new terminal and run:

```bat
go version
terraform version
docker version
kubectl version --client
kind version
```

| Tool | Required |
| --- | --- |
| Go | 1.26 or newer |
| Terraform | 1.5 or newer |
| Docker | Engine running |
| kubectl | client installed |
| kind | installed |

When those match, continue with [build-and-run.md](build-and-run.md).
