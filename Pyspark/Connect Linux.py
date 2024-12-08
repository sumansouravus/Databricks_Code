# Databricks notebook source
# Install Paramiko
%pip install paramiko

# COMMAND ----------

dbutils.library.restartPython()  # Restart after installation

# COMMAND ----------

import paramiko

# SSH details
host = 'localhost'
port = 22
username = 'suman'
password = '------'

# Initialize SSH client
ssh_client = paramiko.SSHClient()

# Automatically add host keys
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    # Connect to the server
    ssh_client.connect(host, port=port, username=username, password=password)

    # Run a command on the server (optional)
    stdin, stdout, stderr = ssh_client.exec_command('ls -l')  # Example command

    # Output the result
    print(stdout.read().decode('utf-8'))

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the SSH connection
    ssh_client.close()

