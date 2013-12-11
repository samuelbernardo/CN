as-update-auto-scaling-group asg  --launch-configuration  lc  --max-size  0 --min-size 0 --availability-zones us-west-2a,us-west-2b,us-west-2c
as-delete-policy dsp --auto-scaling-group asg -f
as-delete-policy isp --auto-scaling-group asg -f
#as-delete-launch-config lc 
