package broker

import (
	"strings"

	"github.com/go-playground/validator/v10"
)

var brokerValidator = validator.New(validator.WithRequiredStructEnabled())

func validRequiredString(value string) bool {
	return brokerValidator.Var(strings.TrimSpace(value), "required") == nil
}

func validNonNegativeInt(value int) bool {
	return brokerValidator.Var(value, "gte=0") == nil
}
