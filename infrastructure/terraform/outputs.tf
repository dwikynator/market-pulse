output "bucket_name" {
  description = "Name of the MarketPulse data bucket."
  value       = aws_s3_bucket.data.id
}

output "bucket_uri" {
  description = "S3 URI of the data bucket."
  value       = "s3://${aws_s3_bucket.data.id}"
}

output "pipeline_policy_arn" {
  description = "IAM policy to attach to a pipeline runtime identity."
  value       = aws_iam_policy.pipeline.arn
}
