locals {
  pipeline_prefixes = [
    "raw",
    "raw/*",
    "curated",
    "curated/*",
    "audit",
    "audit/*",
    "streaming",
    "streaming/*",
  ]

  pipeline_object_arns = [
    "${aws_s3_bucket.data.arn}/raw/*",
    "${aws_s3_bucket.data.arn}/curated/*",
    "${aws_s3_bucket.data.arn}/audit/*",
    "${aws_s3_bucket.data.arn}/streaming/*",
  ]
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid    = "ReadBucketMetadata"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]

    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid     = "ListProjectPrefixes"
    effect  = "Allow"
    actions = ["s3:ListBucket"]

    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = local.pipeline_prefixes
    }
  }

  statement {
    sid    = "ReadAndWriteProjectObjects"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]

    resources = local.pipeline_object_arns
  }

  statement {
    sid       = "UpdateStreamingState"
    effect    = "Allow"
    actions   = ["s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data.arn}/streaming/*"]
  }
}

resource "aws_iam_policy" "pipeline" {
  name        = "${var.project_name}-${var.environment}-pipeline-s3"
  description = "S3 access for the MarketPulse collectors and Spark job."
  policy      = data.aws_iam_policy_document.pipeline.json
}
