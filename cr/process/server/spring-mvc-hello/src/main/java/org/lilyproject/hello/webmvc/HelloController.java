package org.lilyproject.hello.webmvc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/welcome")
public class HelloController {

    private Log log = LogFactory.getLog(HelloController.class);

    @RequestMapping(method = RequestMethod.GET)
    public String printWelcome(ModelMap model) {

        log.debug("This is the hellocontroller saying hello");

        model.addAttribute("message", "Hello World");

        // return the view name
        return "hello";
    }
}
