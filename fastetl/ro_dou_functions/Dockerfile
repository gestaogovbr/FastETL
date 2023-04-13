# docker build -t ghcr.io/economiagovbr/selenium:ro-dou .
# docker push ghcr.io/economiagovbr/selenium:ro-dou
# https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry

FROM selenium/standalone-chrome:4.7.2

USER root

#==============================
# Install extras
#==============================
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
    && apt-get install -yqq python3-pip \
    && pip3 install selenium=="4.7.2" Flask=="2.2.2"

USER seluser

WORKDIR /home/seluser

COPY get_page_source.py .

CMD ["/bin/bash","-c","python3 get_page_source.py & /opt/bin/entry_point.sh"]

EXPOSE 4444 5000

