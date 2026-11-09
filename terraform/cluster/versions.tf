terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = ">= 0.6.0, < 1.0.0"
    }
  }
}

provider "kind" {}
