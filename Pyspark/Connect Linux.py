# Databricks notebook source
# Install paramiko library to handle SSH connections
%pip install paramiko

# COMMAND ----------

# MAGIC  %restart_python

# COMMAND ----------

import paramiko

# Define your server and SSH credentials
hostname = "172.XX.XX.11"
port = 22  # Default SSH port
username = "labex"
private_key_path = "/dbfs/FileStore/id_rsa"  # Use DBFS path for private key

# Create an SSH client object
client = paramiko.SSHClient()

# Automatically add the host key if it's missing
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Connect to the server using the private key
try:
    private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
    client.connect(hostname, port=port, username=username, pkey=private_key)
    print("Successfully connected to the server!")
    
    # You can run commands on the Linux server, for example:
    stdin, stdout, stderr = client.exec_command('ls -l')
    print(stdout.read().decode())  # Output from the command

except Exception as e:
    print(f"Failed to connect: {e}")

finally:
    # Close the SSH connection
    client.close()


# COMMAND ----------

stdin, stdout, stderr = client.exec_command('echo "Hello from Databricks!"')
print(stdout.read().decode())  # Display output

