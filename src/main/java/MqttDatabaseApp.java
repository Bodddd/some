import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MqttDatabaseApp {

    public static void main(String[] args) {

        String brokerUrl = "tcp://broker.hivemq.com:1883";
        String topic = "test/topic";
        String clientId = MqttClient.generateClientId();

        String dbUrl = "jdbc:mysql://localhost:3306/university";
        String dbUser = "root";
        String dbPassword = "2010";

        try {
            MqttClient client = new MqttClient(brokerUrl, clientId);
            client.setCallback(new MqttCallback() {
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String payload = new String(message.getPayload());
                    System.out.println("Message received:\n\t" + payload);
                    writeToDatabase(dbUrl, dbUser, dbPassword, payload);
                }

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection lost: " + cause.getMessage());
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Not used in this example
                }
            });

            client.connect();
            client.subscribe(topic);
            Thread.sleep(60000);

            client.disconnect();
            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeToDatabase(String url, String user, String password, String data) {
        try (Connection con = DriverManager.getConnection(url, user, password)) {
            String query = "INSERT INTO your_table (column1) VALUES (?)";
            try (PreparedStatement pst = con.prepareStatement(query)) {
                pst.setString(1, data);
                pst.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}