package org.lilycms.server.modules.repository;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.specific.SpecificResponder;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.avro.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;

public class AvroServer {
    private String bindAddress;
    private Repository repository;
    private int repositoryPort;
    private int typeManagerPort;

    private SocketServer repositoryServer;
    private SocketServer typeManagerServer;

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

        AvroRepositoryImpl avroRepository = new AvroRepositoryImpl(repository, avroConverter);
        Responder repoResponder = new SpecificResponder(AvroRepository.class, avroRepository);
        repositoryServer = new SocketServer(repoResponder, new InetSocketAddress(bindAddress, repositoryPort));

        AvroTypeManagerImpl avroTypeManager = new AvroTypeManagerImpl(repository.getTypeManager(), avroConverter);
        Responder typeMgrResponder = new SpecificResponder(AvroTypeManager.class, avroTypeManager);
        typeManagerServer = new SocketServer(typeMgrResponder, new InetSocketAddress(bindAddress, typeManagerPort));
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
