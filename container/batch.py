from config import Config
import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels
import datetime
from time import sleep


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

    blob_client.create_container(Config.output_container_name,
                                 fail_on_exist=False)

    # Create output blob url
    output_url = output_sas_url(Config.storage_account_name,
                                Config.output_container_name)

    # Create a client for the batch service
    credentials = batch_auth.SharedKeyCredentials(
        account_name=Config.batch_account_name,
        key=Config.batch_account_key
        )
    batch_client = batch.BatchServiceClient(
        credentials=credentials,
        batch_url=Config.batch_account_url
        )

    # Define docker image
    container_config = batchmodels.ContainerConfiguration(
        container_image_names=['chuanwen/cowsay']
        )

    # Define the virtual machine configuration, we must use a batch container
    # compatible image
    vm_config = batchmodels.VirtualMachineConfiguration(
        image_reference=batch.models.ImageReference(
            publisher='microsoft-azure-batch',
            offer='ubuntu-server-container',
            sku='16-04-lts',
            version='latest'
            ),
        node_agent_sku_id='batch.node.ubuntu 16.04',
        container_configuration=container_config
        )

    try:
        pool = batchmodels.PoolAddParameter(
            id=Config.pool_id,
            vm_size=Config.vm_size,
            virtual_machine_configuration=vm_config,
            target_dedicated_nodes=Config.dedicated_node_count,
            target_low_priority_nodes=Config.low_priority_node_count
            )

        # Create a pool (set of VMs to run tasks) using the vm configuration
        # and add it to the batch service
        batch_client.pool.add(pool=pool)

        # Create the job (A collection of individual tasks that will run on the
        # pool)
        batch_client.job.add(
            batchmodels.JobAddParameter(
                id=Config.job_id,
                pool_info=batchmodels.PoolInformation(pool_id=Config.pool_id)
                )
            )

        # Define settings for the container tasks
        task_container_settings = batch.models.TaskContainerSettings(
            image_name='chuanwen/cowsay'
            )

        tasks = []
        for index in range(10):
            std_out = batchmodels.OutputFile(
                file_pattern='../stdout.txt',
                destination=batchmodels.OutputFileDestination(
                    container=batchmodels.OutputFileBlobContainerDestination(
                        container_url=output_url,
                        path='stdout_{}.txt'.format(index)
                        )
                    ),
                upload_options=batchmodels.OutputFileUploadOptions(
                    upload_condition=(
                        batchmodels.OutputFileUploadCondition.task_completion
                        )
                    )
                )

            # Add a new task to the list
            # An empty command line causes the ENTRYPOINT of the container to
            # be called
            tasks.append(
                batchmodels.TaskAddParameter(
                    id='Task{}'.format(index),
                    command_line='',
                    resource_files=[],
                    output_files=[std_out],
                    container_settings=task_container_settings
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
