package org.lilycms.server.modules.repository;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.specific.SpecificResponder;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.avro.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

public class AvroServer {
    private String bindAddress;
    private Repository repository;
    private int repositoryPort;
    private int typeManagerPort;

    private HttpServer repositoryServer;
    private HttpServer typeManagerServer;

    public AvroServer(String bindAddress, Repository repository, int repositoryPort, int typeManagerPort) {
        this.bindAddress = bindAddress;
        this.repository = repository;
        this.repositoryPort = repositoryPort;
        this.typeManagerPort = typeManagerPort;
    }

    @PostConstruct
    public void start() throws IOException {
        AvroConverter avroConverter = new AvroConverter();
        avroConverter.setRepository(repository);

        AvroLilyImpl avroLily = new AvroLilyImpl(repository, avroConverter);
        Responder repoResponder = new SpecificResponder(AvroLily.class, avroLily);
        repositoryServer = new HttpServer(repoResponder, repositoryPort);
    }
    
    @PreDestroy
    public void stop() {
        repositoryServer.close();
        typeManagerServer.close();
    }

    public int getRepositoryPort() {
        return repositoryServer.getPort();
    }

    public int getTypeManagerPort() {
        return typeManagerServer.getPort();
    }
}
