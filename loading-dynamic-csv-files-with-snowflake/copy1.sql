copy into mytable
from s3://big-time-data/files
credentials=(aws_key_id='$AWS_ACCESS_KEY_ID' 
aws_secret_key='$AWS_SECRET_ACCESS_KEY')
FILE_FORMAT = ( TYPE = CSV );
