FROM stagex/sops:sx2024.08.1@sha256:7d8d51e41c7cab21b8ae75f557961f20405f727a21107d669080e3804d09665c as sops
FROM stagex/libqrencode:sx2024.08.1@sha256:1927d17aaf1ad6a9910380714f0dd12c72c69f9ee1b19668bf4cc5f89cbc2b2d AS libqrencode

# Debian version if needed
FROM rust:slim-bullseye
RUN apt update 
RUN apt install -y git gnupg2 pkg-config nettle-dev libpcsclite-dev clang llvm
RUN apt install -y libnettle8 libpcsclite1 pcscd

# # Alpine version
# FROM rust:alpine
# RUN apk update
# RUN apk add git gnupg pkgconfig nettle-dev clang llvm 
# #libpcsclite-dev pcscd
# RUN apk add nettle pcsc-lite-dev


RUN git clone https://git.distrust.co/public/keyfork
WORKDIR /keyfork
# TODO: verify the commit
# RUN git verify-commit HEAD
RUN cargo install --locked --path crates/keyfork

RUN mkdir /sops-test

COPY --from=sops . /sops-test
COPY --from=libqrencode . /qr-test

RUN export PATH=$PATH:/sops-test/bin/:/qr-test/bin/
CMD ["/bin/sh"]
