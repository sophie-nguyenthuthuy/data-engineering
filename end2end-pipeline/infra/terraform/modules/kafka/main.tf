resource "aws_security_group" "cluster" {
  name        = "${var.name_prefix}-msk"
  description = "MSK Serverless cluster — IAM-authenticated TLS (9098) from within VPC."
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name_prefix}-msk" })
}

resource "aws_vpc_security_group_ingress_rule" "cluster_iam" {
  security_group_id = aws_security_group.cluster.id
  description       = "IAM-auth Kafka clients from within the VPC"
  cidr_ipv4         = var.vpc_cidr_block
  from_port         = 9098
  to_port           = 9098
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "cluster_all" {
  security_group_id = aws_security_group.cluster.id
  description       = "Allow all egress"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${var.name_prefix}-msk"

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.cluster.id]
  }

  # MSK Serverless only supports IAM auth. SASL/SCRAM is MSK Provisioned only.
  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = var.tags
}
