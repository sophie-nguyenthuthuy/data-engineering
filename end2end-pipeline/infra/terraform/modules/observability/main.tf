data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------------------
# Amazon Managed Prometheus — target for the OTel Collector's remote_write.
# ------------------------------------------------------------------------------
resource "aws_prometheus_workspace" "this" {
  alias = "${var.name_prefix}-metrics"
  tags  = var.tags
}

# ------------------------------------------------------------------------------
# Amazon Managed Grafana — admin via AWS SSO / IAM Identity Center.
# ------------------------------------------------------------------------------
resource "aws_iam_role" "grafana" {
  name = "${var.name_prefix}-grafana"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "grafana.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = var.tags
}

data "aws_iam_policy_document" "grafana" {
  statement {
    sid = "ReadAMPAndCloudWatch"
    actions = [
      "aps:QueryMetrics",
      "aps:GetLabels",
      "aps:GetMetricMetadata",
      "aps:GetSeries",
      "aps:ListWorkspaces",
      "aps:DescribeWorkspace",
      "cloudwatch:DescribeAlarmsForMetric",
      "cloudwatch:DescribeAlarmHistory",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:ListMetrics",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:GetMetricData",
      "cloudwatch:GetInsightRuleReport",
      "logs:DescribeLogGroups",
      "logs:GetLogGroupFields",
      "logs:StartQuery",
      "logs:StopQuery",
      "logs:GetQueryResults",
      "logs:GetLogEvents",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "grafana" {
  role   = aws_iam_role.grafana.id
  policy = data.aws_iam_policy_document.grafana.json
}

resource "aws_grafana_workspace" "this" {
  name                     = "${var.name_prefix}-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "SERVICE_MANAGED"
  role_arn                 = aws_iam_role.grafana.arn

  data_sources = ["PROMETHEUS", "CLOUDWATCH"]

  tags = var.tags
}

resource "aws_grafana_role_association" "admins" {
  count        = length(var.grafana_admin_role_arns) > 0 ? 1 : 0
  workspace_id = aws_grafana_workspace.this.id
  role         = "ADMIN"
  # Managed Grafana admin mapping works with SSO user/group IDs, not IAM ARNs —
  # we keep this as a placeholder. Operators set real SSO IDs via tfvars once
  # Identity Center is wired.
  user_ids = var.grafana_admin_role_arns
}
