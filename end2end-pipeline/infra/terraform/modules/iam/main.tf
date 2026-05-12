data "aws_caller_identity" "current" {}

locals {
  # Default to "any branch of this repo" — callers should tighten via allowed_refs.
  refs = length(var.allowed_refs) > 0 ? var.allowed_refs : [
    "repo:${var.github_owner}/${var.github_repo}:ref:refs/heads/main",
    "repo:${var.github_owner}/${var.github_repo}:pull_request",
  ]
}

# GitHub's OIDC provider — one per account; caller can skip if already present.
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
  tags            = var.tags
}

data "aws_iam_policy_document" "gha_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = local.refs
    }
  }
}

# Two roles: a plan role with read-only access, and an apply role with power.
# Plan runs on PRs from forks; apply runs on main only.
resource "aws_iam_role" "plan" {
  name               = "${var.name_prefix}-gha-plan"
  description        = "Used by GitHub Actions terraform plan. Read-only."
  assume_role_policy = data.aws_iam_policy_document.gha_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "plan_readonly" {
  role       = aws_iam_role.plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_iam_role" "apply" {
  name               = "${var.name_prefix}-gha-apply"
  description        = "Used by GitHub Actions terraform apply. Scoped to main."
  assume_role_policy = data.aws_iam_policy_document.gha_assume.json
  tags               = var.tags
}

# Apply gets PowerUserAccess (no IAM). Creating IAM roles from CI is a
# footgun — bootstrap those via a separate admin apply.
resource "aws_iam_role_policy_attachment" "apply_power" {
  role       = aws_iam_role.apply.name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

# Narrow IAM permissions required to manage the modules' roles/policies.
data "aws_iam_policy_document" "apply_iam" {
  statement {
    sid = "ManageModuleRolesAndPolicies"
    actions = [
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:UpdateRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:PassRole",
      "iam:CreatePolicy",
      "iam:DeletePolicy",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:ListPolicyVersions",
      "iam:CreatePolicyVersion",
      "iam:DeletePolicyVersion",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:TagPolicy",
      "iam:UntagPolicy",
    ]
    # Scope to the stack's name prefix so the apply role can't rotate the
    # admin's roles out from under them.
    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.name_prefix}-*",
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.name_prefix}-*",
    ]
  }
}

resource "aws_iam_role_policy" "apply_iam" {
  name   = "${var.name_prefix}-gha-apply-iam"
  role   = aws_iam_role.apply.id
  policy = data.aws_iam_policy_document.apply_iam.json
}
