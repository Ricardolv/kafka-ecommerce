package com.richard.ecommerce;

import com.richard.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_USER_GENERATE_READING_REPORT;

public class GenerateAllReportServelet extends HttpServlet {

    private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        this.batchDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {

        try {

            this.batchDispatcher.send(KafkaTopics.ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS.name(),
                                      ECOMMERCE_USER_GENERATE_READING_REPORT.name(),
                                      new CorrelationId(GenerateAllReportServelet.class.getSimpleName()),
                                      ECOMMERCE_USER_GENERATE_READING_REPORT.name());

            System.out.println("Sent generate report to all users");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Sent generate report");

        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new ServletException(e);
        }
    }

}
