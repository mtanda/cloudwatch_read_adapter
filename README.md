# cloudwatch_read_adapter
CloudWatch remote read adapter for Prometheus.

## Setup
Add following setting to `prometheus.yml`.

```
remote_read:
  - url: http://localhost:9415/read
```

And then, launch read adapter.
```
./cloudwatch_read_adapter
```

## AWS credentials
This read apadter use `AWS SDK for Go`, read credentials from Environment variables, IAM role for Amazon EC2, etc.
Please read following document.

https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html

## Query
Specify CloudWatch GetMetricStatistics parameters as label.
```
{Region="us-east-1",Namespace="AWS/EC2",MetricName="CPUUtilization",InstanceId="i-0123456789abcdefg",Statistics="Average",Period="300"}
```

## Limitation
Now, this plugin doesn't support regex matcher.
