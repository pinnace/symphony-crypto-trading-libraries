from symphony.config import PROXY_PASS, PROXY_USER, AWS_REGION
from typing import List, Optional, Dict
import boto3
from time import sleep
import requests

ec2_resource = boto3.resource('ec2', region_name=AWS_REGION)
ec2_client = boto3.client('ec2', region_name=AWS_REGION)


def start_proxies() -> List[ec2_resource.Instance]:
    """
    Start the proxy servers

    :return: List of EC2 instances
    """
    instances = __get_proxy_instances()
    for instance in instances:
        if instance.state["Name"] == "stopping":
            while instance.state["Name"] == "stopping":
                sleep(0.5)
                instance.load()
        if instance.state["Name"] != "running" and instance.state["Name"] != "terminated":
            ec2_client.start_instances(InstanceIds=[instance.id])
    for instance in instances:
        instance.wait_until_running()
        instance.load()
    return instances


def get_proxy_objects(instances: List[ec2_resource.Instance]) -> List[Dict[str, str]]:
    """
    Create proxy objects from EC2 instances using their attributes

    :param instances: The list of Boto3 EC2 instance objects
    :return: List of proxy objects to use with requests
    """
    proxies = []
    for instance in instances:
        proxies.append(
            {
                'http': 'http://{USER}:{PASS}@{HOST}:8080'.format(USER=PROXY_USER, PASS=PROXY_PASS, HOST=instance.public_dns_name),
                'https': 'http://{USER}:{PASS}@{HOST}:8080'.format(USER=PROXY_USER, PASS=PROXY_PASS, HOST=instance.public_dns_name)
            }
        )
    return proxies


def stop_proxies() -> None:
    """
    Stop all the proxies

    :return: None
    """
    instances = __get_proxy_instances()
    for instance in instances:
        if instance.state["Name"] == "running" and instance.state["Name"] != "terminated":
            instance.stop()
    return


def get_ip(proxy_config: Optional[Dict[str, str]]) -> str:
    """
    Get external IP

    :param proxy_config: Optionally supply a proxy config
    :return: IP
    """
    if proxy_config:
        return requests.get('https://checkip.amazonaws.com', proxies=proxy_config).text.strip()
    return requests.get('https://checkip.amazonaws.com').text.strip()


def __get_proxy_instances(tag_key: Optional[str] = "purpose", tag_value: Optional[str] = "lambda-proxy") -> list:
    """
    Get instances that are tagged as being lambda proxies

    :param tag_key: Optional key value
    :param tag_value: Optional value
    :return: List of EC2 instances
    """
    instances = []
    for instance in ec2_resource.instances.all():
        for tag in instance.tags:
            if tag['Key'] == tag_key and tag['Value'] == tag_value:
                instances.append(instance)
    return instances
