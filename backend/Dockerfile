FROM python:3.13

RUN apt-get install ca-certificates

# Configure INL certs and environment variables
RUN wget -q -P /usr/local/share/ca-certificates/ https://certstore.inl.gov/pki/CAINLROOT_B64.crt
RUN /usr/sbin/update-ca-certificates
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs/
    
WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install pip_system_certs --trusted-host files.pythonhosted.org --trusted-host pypi.org --no-cache-dir --upgrade -r /code/requirements.txt

COPY . /code

CMD ["uvicorn", "main:app", "--reload"]
