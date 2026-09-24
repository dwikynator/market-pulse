variable "project_name" {
  description = "Short lowercase name used in AWS resource names."
  type        = string
  default     = "market-pulse"
}

variable "environment" {
  description = "Deployment environment represented by this Terraform state."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS Region for regional project resources."
  type        = string
  default     = "ap-southeast-1"
}
