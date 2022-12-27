import boto3
import os

s3 = boto3.resource('s3')

# CREATE S3 BUCKET:
def create_s3_bucket(bucket_name):
    try:
        s3.create_bucket(
            Bucket=bucket_name
        )
        print("S3 bucket created successfully")
    except Exception as e:
        print(e)

# DELETE S3 BUCKET:
def delete_s3_bucket(bucket_name):
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.delete()
        print("Terminated S3 bucket: %s" % bucket_name)
    except Exception as e:
        print(e)

# PRINT S3 BUCKET DETAILS:
def print_s3_bucket_details(bucket):
    print("{name=%s, creation_date=%s}" % (bucket.name, bucket.creation_date))

# VIEW ALL S3 BUCKETS:
def view_all_s3_buckets():
    for bucket in s3.buckets.all():
        print_s3_bucket_details(bucket)

# UPLOAD LOCAL FILE TO S3 BUCKET:
def upload_local_file_to_s3(filename, bucketname, destination_path):
    s3.Bucket(bucketname).upload_file(filename, destination_path+filename)

# UPLOAD DIRECTORY OF MULTIPLES SUB-DIRS TO A S3 BUCKET (FOLDER THAT CONTAINS MULTIPLES SUB-DIRECTORIES AND FILES):
def upload_dir_to_s3(dir_path, bucket_name, s3_path):
    for subdir in os.listdir(dir_path):
        # If the sub-directory is a file, upload it to S3:
        if os.path.isfile(os.path.join(dir_path, subdir)):
            s3.meta.client.upload_file(os.path.join(dir_path, subdir), bucket_name, s3_path+'/'+subdir)
        else:
            # If the sub-directory is a directory, upload it to S3:
            for file in os.listdir(os.path.join(dir_path, subdir)):
                # If the file is not a directory, upload it to S3:
                if not os.path.isdir(os.path.join(dir_path, subdir, file)):
                    s3.meta.client.upload_file(os.path.join(dir_path, subdir, file), bucket_name, os.path.join(s3_path, subdir, file))

# DELETE FILE FROM S3 BUCKET:
def delete_file_from_s3(filename, bucketname, path):
    s3.Bucket(bucketname).Object(path+filename).delete()

# DELETE DIRECTORY FROM S3 BUCKET:
def delete_directory_from_s3_bucket(bucket_name, directory_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=directory_name).delete()