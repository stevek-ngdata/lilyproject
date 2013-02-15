package org.lilyproject.webmvc.frontend;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/welcome")
public class HelloController {

    private Log log = LogFactory.getLog(HelloController.class);

    @Autowired
    Repository repository;

    @PostConstruct
    public void init() {
        log.info("initializing controller");
    }

    @RequestMapping(value = "/welcome", method = RequestMethod.GET)
    public String printWelcome(ModelMap model) throws InterruptedException, RepositoryException {

        log.info("This is the hellocontroller saying hello");

        // proof that we can use the repository by reading an unexisting record
        try {
            repository.read(repository.getIdGenerator().newRecordId());
        } catch (RecordNotFoundException e) {
            log.info("Proof that we can use the repository module!");
        }

        model.addAttribute("message", "Hello World");

        // return the view name
        return "hello";
    }
}
