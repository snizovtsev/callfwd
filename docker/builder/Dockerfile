FROM centos:7
MAINTAINER Sergey Nizovtsev <snizovtsev@gmail.com>

RUN EPEL_RPM_URL="https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm" \
    && yum install -y centos-release-scl "$EPEL_RPM_URL" \
    && yum clean all && rm -rf /var/cache/yum

RUN yum install -y devtoolset-9-toolchain cmake3 \
    && yum clean all && rm -rf /var/cache/yum

RUN yum install -y libsodium-devel zlib-devel openssl-devel jemalloc-devel \
        libevent-devel double-conversion-devel libzstd-devel libicu-devel \
        systemd-devel \
    && yum clean all && rm -rf /var/cache/yum

ENV TBB_VERSION=20191006oss
RUN TBB_URL=https://github.com/oneapi-src/oneTBB/releases/download/2019_U9 \
    TBB_ARCHIVE=tbb2019_${TBB_VERSION}_lin.tgz \
    && echo "Downloading $TBB_URL/$TBB_ARCHIVE" \
    && curl -L "$TBB_URL/$TBB_ARCHIVE" -o "/tmp/$TBB_ARCHIVE" \
    && tar xf "/tmp/$TBB_ARCHIVE" -C /opt \
    && rm -f "/tmp/$TBB_ARCHIVE"
ENV CMAKE_PREFIX_PATH=/opt/tbb2019_$TBB_VERSION

RUN echo 'source /opt/rh/devtoolset-9/enable' > /etc/profile.d/devtoolset.sh \
    && ln -s /usr/bin/cmake3 /usr/local/bin/cmake
