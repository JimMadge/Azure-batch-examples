from config import Config
import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels
import datetime
from time import sleep
import os


def upload_to_blob(file_path, client, container):
    """
    Upload a file to Azure Blob storage.

    Args:
        file_path (str): Path of the file to upload.
        client (`azure.storage.blob.BlockBlobService`): Blob service
        container (str): Name for the container

    Returns:
        (`azure.batch.models.ResourceFile`): A resource file object containing
            the location of hte file uploaded.
    """
    # Get name of file
    file_name = os.path.basename(file_path)
    # Create blob from the file, using it's file name
    client.create_blob_from_path(container, file_name, file_path)

    # Create a shared access signature (SAS) token to let nodes in the pool
    # access the data in blob storage
    sas_token = client.generate_blob_shared_access_signature(
        container,
        file_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2)
        )

    # Create a uri for the blob.
    sas_url = client.make_blob_url(
        container,
        file_name,
        sas_token=sas_token
        )

    return batchmodels.ResourceFile(
        http_url=sas_url,
        file_path=file_name
        )


def output_sas_url(storage_account_name, container_name):
    """
    Generate the SAS url for uploading output.

    Args:
        storage_account_name (str): Name of the storage account to upload to.
        container_name (str): Name of the blob container (in the storage
            account) to upload to.
    """
    output_sas_token = blob_client.generate_container_shared_access_signature(
        container_name,
        permission=azureblob.BlobPermissions.WRITE,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2)
        )
    output_url = 'https://{}.blob.core.windows.net/{}?{}'.format(
        storage_account_name,
        container_name,
        output_sas_token
        )

    return output_url


def wait_for_tasks(job_id):
    """
    Wait for all tasks to finish

    Args:
        job_id (str): Name of the job.
    """
    print("Waiting for all tasks to finish\n")
    while(True):
        tasks = batch_client.task.list(job_id)
        if all([task.state == batchmodels.TaskState.completed
                for task in tasks]):
            print()
            break
        else:
            print('.', end='', flush=True)
            sleep(3)


if __name__ == '__main__':
    # Create a client for blob storage on the storage account
    blob_client = azureblob.BlockBlobService(
        account_name=Config.storage_account_name,
        account_key=Config.storage_account_key
        )

    # Create blob containers to the script and output
    blob_client.create_container(Config.script_container_name,
                                 fail_on_exist=False)
    blob_client.create_container(Config.output_container_name,
                                 fail_on_exist=False)

    # Create output blob url
    output_url = output_sas_url(Config.storage_account_name,
                                Config.output_container_name)

    # Upload program
    script = upload_to_blob('./find_prime.py', blob_client,
                            Config.script_container_name)

    # Create a client for the batch service
    credentials = batch_auth.SharedKeyCredentials(
        account_name=Config.batch_account_name,
        key=Config.batch_account_key
        )
    batch_client = batch.BatchServiceClient(
        credentials=credentials,
        batch_url=Config.batch_account_url
        )

    # Define the virtual machine configuration
    vm_config = batchmodels.VirtualMachineConfiguration(
        image_reference=batchmodels.ImageReference(
            publisher='Canonical',
            offer='UbuntuServer',
            sku='18.04-LTS',
            version='latest'
            ),
        node_agent_sku_id='batch.node.ubuntu 18.04'
        )

    try:
        # Create a pool (set of VMs to run tasks) using the vm configuration
        # and add it to the batch service
        batch_client.pool.add(
            pool=batchmodels.PoolAddParameter(
                id=Config.pool_id,
                vm_size=Config.vm_size,
                virtual_machine_configuration=vm_config,
                target_dedicated_nodes=Config.dedicated_node_count,
                target_low_priority_nodes=Config.low_priority_node_count
                )
            )

        # Definition of output files to upload
        output_file = batchmodels.OutputFile(
            file_pattern='primes_*.txt',
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=output_url
                    )
                ),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=(
                    batchmodels.OutputFileUploadCondition.task_success
                    )
                )
            )
        # Upload stdout and stderr if the task fails
        std_files = batchmodels.OutputFile(
            file_pattern='../std*.txt',
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=output_url
                    )
                ),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=(
                    batchmodels.OutputFileUploadCondition.task_failure
                    )
                )
            )

        # Create the job (A collection of individual tasks that will run on the
        # pool)
        batch_client.job.add(
            batchmodels.JobAddParameter(
                id=Config.job_id,
                pool_info=batchmodels.PoolInformation(pool_id=Config.pool_id)
                )
            )

        # Upper and lower bound generator
        def upper_and_lower(n):
            iteration = 0
            lower = 1
            upper = 1000000
            while iteration < n:
                yield lower, upper
                iteration += 1
                lower += 1000000
                upper += 1000000
        # Create a set of tasks
        tasks = []
        for index, (lower, upper) in enumerate(upper_and_lower(10)):
            # Construct command line argument
            python_command = '{} {} {}'.format(script.file_path, lower, upper)
            command = '/bin/bash -c "{} > primes_{}.txt"'.format(
                python_command,
                index
                )
            tasks.append(
                batchmodels.TaskAddParameter(
                    id='Task{}'.format(index),
                    command_line=command,
                    resource_files=[script],
                    output_files=[output_file, std_files]
                    )
                )
        # Add tasks to batch client, under the job specified previously
        batch_client.task.add_collection(
            job_id=Config.job_id,
            value=tasks
            )

        # Wait for tasks to complete
        wait_for_tasks(Config.job_id)

    except batchmodels.BatchErrorException as err:
        batch_client.job.delete(Config.job_id)
        batch_client.pool.delete(Config.pool_id)
        raise err

    # Clean up
    batch_client.job.delete(Config.job_id)
    batch_client.pool.delete(Config.pool_id)
