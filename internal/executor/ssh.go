package executor

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
)

type SSHExecutor struct {
	// Можно кэшировать соединения, но для простоты пока без кэша
}

func NewSSHExecutor() *SSHExecutor {
	return &SSHExecutor{}
}

// Execute подключается по SSH и выполняет команду.
// Возвращает exit code, stdout, stderr, error.
func (e *SSHExecutor) Execute(targetAddr string, targetPort int, user, keyPath, command string) (int, string, string, error) {
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to read key %s: %v", keyPath, err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to parse key: %v", err)
	}

	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // для тестов, в проде нужна проверка
		Timeout:         10 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", targetAddr, targetPort)
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to dial %s: %v", addr, err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return -1, "", "", fmt.Errorf("failed to create session: %v", err)
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	err = session.Run(command)
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*ssh.ExitError); ok {
			exitCode = exitErr.ExitStatus()
		} else {
			return -1, stdout.String(), stderr.String(), fmt.Errorf("session run error: %v", err)
		}
	}

	return exitCode, stdout.String(), stderr.String(), nil
}
