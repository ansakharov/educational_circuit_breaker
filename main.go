package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Status int

const (
	CLOSED   Status = 1
	OPEN     Status = 2
	HALFOPEN Status = 3
)

type CircuitBreaker struct {
	mu sync.Mutex
	// CLOSED - work!, OPEN - fail!, HALFOPEN - work until fail!
	state Status
	// Длинна отслеживаемого хвоста запросов
	recordLength int
	// Сколько времени у CB восстановиться
	timeout time.Duration

	lastAttemptedAt time.Time
	// Процент запросов после которого открывается CB
	percentile float64
	// Buffer хранит данные о результатах запроса
	buffer []bool
	// Pos увеличивается для каждого след запроса, потом сбрасывает в 0
	pos int
	// Сколько успешных запросов надо сделать подряд, чтобы перейти в CLOSED
	recoveryRequests int
	// Сколько успешных запросов в HALFOPEN уже сделано
	successCount int
}

func NewCircuitBreaker(recordLength int, timeout time.Duration, percentile float64, recoveryRequests int) *CircuitBreaker {
	return &CircuitBreaker{
		state:            CLOSED,
		recordLength:     recordLength,
		timeout:          timeout,
		percentile:       percentile,
		buffer:           make([]bool, recordLength),
		pos:              0,
		recoveryRequests: recoveryRequests,
		successCount:     0,
	}
}

func (c *CircuitBreaker) Call(service func() error) (err error) {
	c.setActualState()

	if c.getState() == OPEN {
		return errors.New("CB IS OPEN")
	}

	err = service()
	if err != nil {
		c.onError()
		return err
	}

	c.onSuccess()

	return nil
}

func (c *CircuitBreaker) getState() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *CircuitBreaker) setActualState() {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case OPEN:
		if elapsed := time.Since(c.lastAttemptedAt); elapsed > c.timeout {
			fmt.Printf("\nSwitching to HALFOPEN state\n")
			c.state = HALFOPEN
		}

	case HALFOPEN:
		if c.successCount > c.recoveryRequests {
			fmt.Printf("\nSwitching to CLOSED state\n")

			c.state = CLOSED
			c.buffer = make([]bool, c.recordLength) // сбрасываем буфер
			c.pos = 0                               // сбрасываем позицию
			c.successCount = 0                      // сбрасываем счетчик успешных запросов
		}
	case CLOSED:
		failureCount := 0

		for _, failed := range c.buffer {
			if failed {
				failureCount++
			}
		}

		if float64(failureCount)/float64(c.recordLength) >= c.percentile {
			fmt.Printf("\nSwitching to OPEN state due to exceeding percentile\n\n")

			c.state = OPEN
			c.lastAttemptedAt = time.Now()
		}
	}
}

func (c *CircuitBreaker) onSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.buffer[c.pos] = false
	c.pos = (c.pos + 1) % c.recordLength

	if c.state == HALFOPEN {
		c.successCount++
	}
}

func (c *CircuitBreaker) onError() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.buffer[c.pos] = true
	c.pos = (c.pos + 1) % c.recordLength

	if c.state == HALFOPEN {
		fmt.Printf("\nSwitching to OPEN state\n")
		c.state = OPEN
		c.successCount = 0
	}

	c.lastAttemptedAt = time.Now()
	c.successCount = 0
}

func main() {
	// Инициализация circuit breaker
	cb := NewCircuitBreaker(100, 2*time.Second, 0.30, 10)

	var err error
	successfulService := func() error {
		return nil
	}

	failingService := func() error {
		return errors.New("service error")
	}

	// Исполняем успешные запросы
	fmt.Println("Sending successful requests...")
	for i := 0; i < 80; i++ {
		if err = cb.Call(successfulService); err != nil {
			fmt.Printf("Service call failed: %s\n", err.Error())
		}
		fmt.Println(i, " ok ")

	}

	// Исполняем запросы с ошибкой
	fmt.Println("\nSending failing requests...\n")

	for i := 0; i < 40; i++ {
		if err = cb.Call(failingService); err != nil {
			fmt.Printf("%d Service call failed: %s\n", i, err.Error())
		}
	}

	// Ожидаем, чтобы CircuitBreaker перешел в half-open state
	fmt.Printf("\nWaiting for circuit breaker to switch to half-open state...\n")
	time.Sleep(3 * time.Second)

	// Исполняем запросы для перехода в closed state
	fmt.Println("Sending successful requests to recover...")
	for i := 0; i < 15; i++ {
		if err = cb.Call(successfulService); err != nil {
			fmt.Printf("Service call failed: %s\n", err.Error())
		}

		fmt.Printf("%d ok\n", i)
	}

	// Исполняем запросы с ошибкой для перехода обратно в open state
	fmt.Printf("\nSending failing requests to switch back to open state 1 ...\n\n")
	for i := 0; i < 40; i++ {
		if err = cb.Call(failingService); err != nil {
			fmt.Printf("%d Service call failed: %s\n", i, err.Error())
		}
	}

	// Ожидаем, чтобы CircuitBreaker перешел в half-open state
	fmt.Printf("\nWaiting for circuit breaker to switch to half-open state...\n")
	time.Sleep(3 * time.Second)

	// Исполняем запросы с ошибкой для перехода обратно в open state
	fmt.Printf("\nSending failing requests to switch back to open state 2 ...\n")
	for i := 0; i < 10; i++ {
		if err = cb.Call(failingService); err != nil {
			fmt.Printf("%d Service call failed: %s\n", i, err.Error())
		}
	}
}
