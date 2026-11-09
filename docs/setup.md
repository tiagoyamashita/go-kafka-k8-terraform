# Setup

Install the tools this repo uses on Windows, then follow [build-and-run.md](build-and-run.md).

This machine currently reports Go 1.18.1 and Terraform 0.13.4. The repo needs Go 1.26 or newer and Terraform 1.5 or newer. `-chdir` does not exist in Terraform 0.13, which is why `terraform -chdir=terraform/cluster init` fails.

Run the installers from an Administrator terminal. After each install, close that terminal and open a new one so `PATH` picks up the new binary.

## See what is already installed

```bat
go version
terraform version
podman info
kind version
kubectl version --client
where go
where terraform
where podman
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

## Podman

kind runs the Kubernetes node as a Podman container. Leave Docker Desktop uninstalled. The Terraform kind provider selects Podman only when the `docker` command is absent.

```bat
winget install -e --id RedHat.Podman
winget install -e --id RedHat.Podman-Desktop
```

Open a new terminal, create the Podman machine, and start it:

```bat
podman machine init
podman machine start
podman info
```

Podman Desktop can create and start that machine for you. `podman info` should show the engine running before Terraform or kind runs.

Tell kind to use Podman, including for later terminals:

```bat
setx KIND_EXPERIMENTAL_PROVIDER podman
```

`setx` applies to new terminals. In the current one, also run `set KIND_EXPERIMENTAL_PROVIDER=podman`.

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
podman info
kubectl version --client
kind version
echo %KIND_EXPERIMENTAL_PROVIDER%
```

| Tool | Required |
| --- | --- |
| Go | 1.26 or newer |
| Terraform | 1.5 or newer |
| Podman | Machine running, and `podman info` succeeds |
| KIND_EXPERIMENTAL_PROVIDER | `podman` |
| kubectl | client installed |
| kind | installed |

When those match, continue with [build-and-run.md](build-and-run.md).
