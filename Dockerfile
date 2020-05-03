FROM dipcode/centos7-python36

RUN yum update -y && \
    yum install -y libXext libSM libXrender

ADD requirements.txt /
ADD src/distribute.py /
//AWS credentials
ENV AWS_DEFAULT_REGION=us-west-2


RUN pip install -r /requirements.txt
ENTRYPOINT ["python", "/distribute.py"]
