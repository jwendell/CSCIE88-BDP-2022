FROM fedora:36

RUN dnf -y install --nodocs --setopt=install_weak_deps=False httpd

CMD ["/usr/sbin/httpd", "-DFOREGROUND"]
