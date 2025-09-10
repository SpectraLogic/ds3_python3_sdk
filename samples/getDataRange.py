import os
import tempfile

from ds3 import ds3, ds3Helpers,ds3network
from ds3.ds3 import *
import time


# This example retrieves specified bytes=53687091100-53687091115 in the file in a specific bucket.
# The output will be written to tempname file.


client = ds3.Client("<host>", ds3.Credentials("<access id", "<secret key>"))

# create a dictionary to map bucket names to object names
object_dict={}
helper = ds3Helpers.Helper(client=client)



file_path = os.path.join(os.path.dirname(str(__file__)),  "<file-name>")
bucketName = "<bucket-name>"
fileName = "<file-name>"
fd, tempname = tempfile.mkstemp()
print (tempname) 
f = open(tempname, "wb")
bucketObjects = client.get_service(ds3.GetServiceRequest())
print(bucketObjects)

# Create a GetObjectRequest and set the range header to retrieve only those bytes.
# You can specify multiple ranges by separating them with commas. Example:
#   req.headers['Range'] = 'bytes=0-1,3-4'
req = ds3.GetObjectRequest(bucketName, fileName, f)
req.headers['Range'] = 'bytes=53687091100-53687091115'
start_time = time.time()
getObjectResult = client.get_object(request= req)


f.close()
os.close(fd)

print(getObjectResult.response.status)
end_time = time.time()
elapsed_time_total = end_time - start_time
print(f"Total elapsed time: {elapsed_time_total} seconds")
