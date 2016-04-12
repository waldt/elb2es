Simple lambda script to load ELB S3 logs into ES via lambda

# setup  
* install the requirements via PIP in a virtual env
* ZIP elb2es.py and the lib/python2.7/site-packages/*
* upload to a lambda function.
* Determine how much RAM you may need to run this(trial and error?) set the timeout accordingly.
    * It seems I can do around 1M lines give or take within 5 minutes(440MB file)
    * Tune your function accordingly. I don't believe ES is a bottleneck
* Configure an S3 event source for new log files.
* Lambda function will run and then push new events to ES.
* Only tested on HTTP/HTTPS listeners
