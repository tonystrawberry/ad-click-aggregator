output "glue_job_name" {
  value = aws_glue_job.reconciliation.name
}

output "schedule_name" {
  value = aws_scheduler_schedule.reconciliation.name
}
