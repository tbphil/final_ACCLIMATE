FROM node:alpine

# Add missing packages
RUN apk --no-check-certificate add wget ca-certificates

# Configure INL certs and environment variables
RUN wget -q -P /usr/local/share/ca-certificates/ https://certstore.inl.gov/pki/CAINLROOT_B64.crt
RUN /usr/sbin/update-ca-certificates
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs/

WORKDIR /front

COPY . /front

RUN --mount=type=cache,target=/root/.npm npm install -g @angular/cli
RUN --mount=type=cache,target=/root/.npm npm install

RUN npm install

EXPOSE 4200

CMD ["npm", "start", "--", "--host=0.0.0.0"]
