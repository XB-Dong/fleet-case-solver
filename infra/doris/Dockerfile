FROM apache/doris:4.0.3-all-slim
# Laptop-sized lab only. Preserve vendor config, reduce FE heap and cap BE memory.
RUN sed -i 's/-Xmx8192m/-Xmx768m/g;s/-Xms8192m/-Xms256m/g' /opt/apache-doris/fe/conf/fe.conf \
 && printf '\nmem_limit = 2600m\n' >> /opt/apache-doris/be/conf/be.conf
