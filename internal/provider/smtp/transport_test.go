package smtp

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestClientTransportCompletesImplicitTLSLoginAndData(t *testing.T) {
	server := startScriptedServer(t, serverAccepted)
	transport := newTestTransport(t, server)
	material := smtpTestMaterial()

	if err := transport.Deliver(context.Background(), material); err != nil {
		t.Fatalf("deliver through scripted SMTP server: %v", err)
	}
	observation := <-server.observations
	if observation.username != "sender@example.com" ||
		observation.password != "authorization-code" ||
		observation.mailFrom != "sender@example.com" ||
		observation.recipient != "recipient@example.com" {
		t.Fatal("scripted SMTP server observed unexpected authentication or envelope fields")
	}
	if !strings.Contains(observation.data, "Subject: test") ||
		!strings.Contains(observation.data, "..leading-dot") {
		t.Fatalf("SMTP DATA was not transferred/dot-stuffed: %q", observation.data)
	}
}

func TestClientTransportPreservesKnownRecipientRejection(t *testing.T) {
	server := startScriptedServer(t, serverRejectRecipient)
	transport := newTestTransport(t, server)
	err := transport.Deliver(context.Background(), smtpTestMaterial())
	var exchange *ExchangeError
	if !errors.As(err, &exchange) || exchange.Phase != PhaseRecipient || exchange.StatusCode != 550 {
		t.Fatalf("recipient error = %#v, want RECIPIENT/550", err)
	}
}

func TestClientTransportMarksLostFinalDataResponseAsAmbiguous(t *testing.T) {
	server := startScriptedServer(t, serverDropAfterData)
	transport := newTestTransport(t, server)
	err := transport.Deliver(context.Background(), smtpTestMaterial())
	var exchange *ExchangeError
	if !errors.As(err, &exchange) || exchange.Phase != PhaseDataCommit ||
		exchange.StatusCode != 0 {
		t.Fatalf("DATA commit error = %#v, want ambiguous network failure", err)
	}
}

type serverBehavior uint8

const (
	serverAccepted serverBehavior = iota
	serverRejectRecipient
	serverDropAfterData
)

type smtpObservation struct {
	username  string
	password  string
	mailFrom  string
	recipient string
	data      string
}

type scriptedServer struct {
	host         string
	port         uint16
	roots        *x509.CertPool
	observations chan smtpObservation
}

func startScriptedServer(t *testing.T, behavior serverBehavior) scriptedServer {
	t.Helper()
	certificate, roots := testServerCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	})
	if err != nil {
		t.Fatalf("listen scripted SMTP server: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	portValue := listener.Addr().(*net.TCPAddr).Port
	server := scriptedServer{
		host:         "localhost",
		port:         uint16(portValue),
		roots:        roots,
		observations: make(chan smtpObservation, 1),
	}
	result := make(chan error, 1)
	go func() {
		result <- serveSMTPConversation(listener, behavior, server.observations)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case serveErr := <-result:
			if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
				t.Errorf("scripted SMTP server: %v", serveErr)
			}
		case <-time.After(time.Second):
			t.Error("scripted SMTP server did not stop")
		}
	})
	return server
}

func serveSMTPConversation(
	listener net.Listener,
	behavior serverBehavior,
	observations chan<- smtpObservation,
) error {
	connection, err := listener.Accept()
	if err != nil {
		return err
	}
	defer connection.Close()
	reader := bufio.NewReader(connection)
	writer := bufio.NewWriter(connection)
	write := func(response string) error {
		if _, err := writer.WriteString(response); err != nil {
			return err
		}
		return writer.Flush()
	}
	read := func(prefix string) (string, error) {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		if !strings.HasPrefix(strings.ToUpper(line), prefix) {
			return "", fmt.Errorf("command %q does not start with %s", line, prefix)
		}
		return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
	}

	if err := write("220 localhost ESMTP ready\r\n"); err != nil {
		return err
	}
	if _, err := read("EHLO "); err != nil {
		return err
	}
	if err := write("250-localhost\r\n250-AUTH LOGIN PLAIN\r\n250 8BITMIME\r\n"); err != nil {
		return err
	}
	if _, err := read("AUTH LOGIN"); err != nil {
		return err
	}
	if err := write("334 VXNlcm5hbWU6\r\n"); err != nil {
		return err
	}
	usernameLine, err := read("")
	if err != nil {
		return err
	}
	if err := write("334 UGFzc3dvcmQ6\r\n"); err != nil {
		return err
	}
	passwordLine, err := read("")
	if err != nil {
		return err
	}
	if err := write("235 2.7.0 authenticated\r\n"); err != nil {
		return err
	}
	mailLine, err := read("MAIL FROM:")
	if err != nil {
		return err
	}
	if err := write("250 2.1.0 sender accepted\r\n"); err != nil {
		return err
	}
	recipientLine, err := read("RCPT TO:")
	if err != nil {
		return err
	}
	if behavior == serverRejectRecipient {
		return write("550 5.1.1 recipient rejected\r\n")
	}
	if err := write("250 2.1.5 recipient accepted\r\n"); err != nil {
		return err
	}
	if _, err := read("DATA"); err != nil {
		return err
	}
	if err := write("354 end with dot\r\n"); err != nil {
		return err
	}
	var data strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if line == ".\r\n" {
			break
		}
		data.WriteString(line)
	}
	username, err := decodeAuthLine(usernameLine)
	if err != nil {
		return err
	}
	password, err := decodeAuthLine(passwordLine)
	if err != nil {
		return err
	}
	observations <- smtpObservation{
		username:  username,
		password:  password,
		mailFrom:  smtpPath(mailLine),
		recipient: smtpPath(recipientLine),
		data:      data.String(),
	}
	if behavior == serverDropAfterData {
		return nil
	}
	if err := write("250 2.0.0 queued\r\n"); err != nil {
		return err
	}
	if _, err := read("QUIT"); err != nil {
		return err
	}
	return write("221 2.0.0 bye\r\n")
}

func newTestTransport(t *testing.T, server scriptedServer) *ClientTransport {
	t.Helper()
	config := validTestConfig()
	config.Host = server.host
	config.Port = server.port
	transport, err := newClientTransport(config, server.roots)
	if err != nil {
		t.Fatalf("new test SMTP transport: %v", err)
	}
	return transport
}

func smtpTestMaterial() ports.DeliveryMaterial {
	return ports.DeliveryMaterial{
		EnvelopeFrom: "sender@example.com",
		EnvelopeTo:   "recipient@example.com",
		MIMEMessage: []byte(
			"From: sender@example.com\r\n" +
				"To: recipient@example.com\r\n" +
				"Subject: test\r\n\r\n" +
				"first line\r\n.leading-dot\r\n",
		),
	}
}

func smtpPath(command string) string {
	start := strings.IndexByte(command, '<')
	end := strings.LastIndexByte(command, '>')
	if start < 0 || end <= start {
		return ""
	}
	return command[start+1 : end]
}

func decodeAuthLine(value string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(value)
	return string(decoded), err
}

func testServerCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate TLS key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate certificate serial: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certificateDER, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		&template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatalf("create TLS certificate: %v", err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		t.Fatalf("parse TLS key pair: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("append test root certificate")
	}
	return certificate, roots
}

func TestScriptedServerPortFitsSMTPConfig(t *testing.T) {
	// Guard the conversion used by the test server on platforms with unusual
	// listener behavior; production configuration is already a uint16.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	if port <= 0 || port > 65535 {
		t.Fatalf("invalid ephemeral port %s", strconv.Itoa(port))
	}
}
