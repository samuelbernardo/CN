#as-create-launch-config lc --image-id ami-4ab4d17a --instance-type t1.micro
as-update-auto-scaling-group asg  --launch-configuration  lc  --max-size  5 --min-size 1 --availability-zones us-west-2a,us-west-2b,us-west-2c
as-put-scaling-policy dsp --auto-scaling-group asg --adjustment=-1 --type ChangeInCapacity --cooldown 300
as-put-scaling-policy isp --auto-scaling-group asg --adjustment=1 --type ChangeInCapacity --cooldown 300

#mon-put-metric-alarm --alarm-name my-alarm --metric-name CPUUtilization --namespace AWS/EC2 --statistic Average  --period 60 --threshold 50 --comparison-operator GreaterThanThreshold  --evaluation-periods 3  --unit Percent --alarm-actions arn:aws:autoscaling:us-west-2:022585699950:scalingPolicy:f5ca3dc3-eee6-45a2-819f-f23d772592e7:autoScalingGroupName/asg:policyName/isp
 
