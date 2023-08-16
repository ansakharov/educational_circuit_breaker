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
	State Status
	// Длинна отслеживаемого хвоста запросов
	RecordLength int
	// Сколько времени у CB восстановиться
	Timeout time.Duration

	LastAttemptedAt time.Time
	// Процент запросов после которого открывается CB
	Percentile float64
	// Buffer хранит данные о результатах запроса
	Buffer []bool
	// Pos увеличивается для каждого след запроса, потом сбрасывает в 0
	Pos int
	// Сколько успешных запросов надо сделать подряд, чтобы перейти в CLOSED
	RecoveryRequests int
	// Сколько успешных запросов в HALFOPEN уже сделано
	SuccessCount int
}

func NewCircuitBreaker(recordLength int, timeout time.Duration, percentile float64, recoveryRequests int) *CircuitBreaker {
	return &CircuitBreaker{
		State:            CLOSED,
		RecordLength:     recordLength,
		Timeout:          timeout,
		Percentile:       percentile,
		Buffer:           make([]bool, recordLength),
		Pos:              0,
		RecoveryRequests: recoveryRequests,
		SuccessCount:     0,
	}
}

func (c *CircuitBreaker) Call(service func() error) error {
	c.mu.Lock()
	// only OPEN
	if c.State == OPEN {
		if elapsed := time.Since(c.LastAttemptedAt); elapsed > c.Timeout {
			fmt.Printf("\nSWITCHING TO HALFOPEN\n")
			c.State = HALFOPEN
			c.SuccessCount = 0
		} else {
			c.mu.Unlock()
			return errors.New("CB IS OPEN")
		}
		c.mu.Unlock()
	} else {
		c.mu.Unlock()
	}

	err := service()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Buffer[c.Pos] = err != nil
	c.Pos = (c.Pos + 1) % c.RecordLength
	// 0 f, 1 f, 2 t, 3 f, 4 f, 5t, 6t ... 36t

	// only HALFOPEN
	if c.State == HALFOPEN {
		if err != nil {
			fmt.Printf("\nSwitching back to open state due to an error\n")

			c.State = OPEN
			c.LastAttemptedAt = time.Now()
			c.SuccessCount = 0 // сбрасываем счетчик успешных запросов
		} else {
			c.SuccessCount++
			if c.SuccessCount > c.RecoveryRequests {
				fmt.Printf("\nSwitching to closed state\n")

				c.Reset()
			}
		}
		return err
	}

	// only CLOSED
	failureCount := 0
	for _, failed := range c.Buffer {
		if failed {
			failureCount++
		}
	}
	if float64(failureCount)/float64(c.RecordLength) >= c.Percentile {
		fmt.Printf("\nSwitching to open state due to exceeding percentile\n\n")

		c.State = OPEN
		c.LastAttemptedAt = time.Now()
	}

	return err
}

func (c *CircuitBreaker) Reset() {
	c.State = CLOSED
	c.Buffer = make([]bool, c.RecordLength) // сбрасываем буфер
	c.Pos = 0                               // сбрасываем позицию
	c.SuccessCount = 0                      // сбрасываем счетчик успешных запросов
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
