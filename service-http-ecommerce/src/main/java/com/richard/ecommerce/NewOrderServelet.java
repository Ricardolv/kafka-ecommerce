package com.richard.ecommerce;

import com.richard.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_NEW_ORDER;

public class NewOrderServelet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        this.orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {

        try {
            // we are not caring about any security issues, we are only
            // showing how to use http as a starting point
            var orderId = req.getParameter("uuid");
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, email, amount);

            try (var database = new NewOrderDatabase()) {

                var message = "New order sent successfully.";

                if (database.sabeNew(order)) {
                    orderDispatcher.send(ECOMMERCE_NEW_ORDER.name(), email, new CorrelationId(NewOrderServelet.class.getSimpleName()), order);

                } else {
                    message = "Old order sreceived.";
                }

                System.out.println(message);
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().println(message);
            }


        } catch (ExecutionException | InterruptedException | IOException | SQLException e) {
           throw new ServletException(e);
        }

    }

}
