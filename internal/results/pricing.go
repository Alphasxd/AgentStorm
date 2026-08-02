package results

import (
	"fmt"
	"math/big"
)

const costScale = 12

func tokenCostUSD(tokens int64, pricePerMillion string) (string, error) {
	price, ok := new(big.Rat).SetString(pricePerMillion)
	if !ok || price.Sign() < 0 {
		return "", fmt.Errorf("invalid USD price")
	}
	cost := new(big.Rat).Mul(price, new(big.Rat).SetInt64(tokens))
	cost.Quo(cost, big.NewRat(1_000_000, 1))
	return cost.FloatString(costScale), nil
}

func costsForTokens(
	pricing *RunPricing, inputTokens, outputTokens int64,
) (inputCost, outputCost, totalCost *string, err error) {
	if pricing == nil {
		return nil, nil, nil, nil
	}
	input, err := tokenCostUSD(inputTokens, pricing.InputUSDPerMillionTokens)
	if err != nil {
		return nil, nil, nil, err
	}
	output, err := tokenCostUSD(outputTokens, pricing.OutputUSDPerMillionTokens)
	if err != nil {
		return nil, nil, nil, err
	}
	total, err := addDecimalStrings(input, output)
	if err != nil {
		return nil, nil, nil, err
	}
	return &input, &output, &total, nil
}

func addDecimalStrings(left, right string) (string, error) {
	leftValue, ok := new(big.Rat).SetString(left)
	if !ok {
		return "", fmt.Errorf("invalid decimal")
	}
	rightValue, ok := new(big.Rat).SetString(right)
	if !ok {
		return "", fmt.Errorf("invalid decimal")
	}
	return new(big.Rat).Add(leftValue, rightValue).FloatString(costScale), nil
}

func subtractDecimalStrings(left, right string) (string, error) {
	leftValue, ok := new(big.Rat).SetString(left)
	if !ok {
		return "", fmt.Errorf("invalid decimal")
	}
	rightValue, ok := new(big.Rat).SetString(right)
	if !ok {
		return "", fmt.Errorf("invalid decimal")
	}
	return new(big.Rat).Sub(leftValue, rightValue).FloatString(costScale), nil
}

func decimalPercentDelta(baseline, candidate string) (*float64, error) {
	baselineValue, ok := new(big.Rat).SetString(baseline)
	if !ok {
		return nil, fmt.Errorf("invalid decimal")
	}
	if baselineValue.Sign() == 0 {
		return nil, nil
	}
	candidateValue, ok := new(big.Rat).SetString(candidate)
	if !ok {
		return nil, fmt.Errorf("invalid decimal")
	}
	delta := new(big.Rat).Sub(candidateValue, baselineValue)
	delta.Quo(delta, baselineValue)
	delta.Mul(delta, big.NewRat(100, 1))
	value, _ := delta.Float64()
	return &value, nil
}
