package results

import "testing"

func TestCostAccountingUsesFixedPrecisionDecimals(t *testing.T) {
	pricing := &RunPricing{
		InputUSDPerMillionTokens:  "2.5",
		OutputUSDPerMillionTokens: "10",
	}
	input, output, total, err := costsForTokens(pricing, 1000, 500)
	if err != nil {
		t.Fatal(err)
	}
	if input == nil || *input != "0.002500000000" ||
		output == nil || *output != "0.005000000000" ||
		total == nil || *total != "0.007500000000" {
		t.Fatalf("unexpected costs: input=%v output=%v total=%v", input, output, total)
	}

	delta, err := subtractDecimalStrings("0.009000000000", "0.007500000000")
	if err != nil || delta != "0.001500000000" {
		t.Fatalf("unexpected cost delta %q: %v", delta, err)
	}
	percent, err := decimalPercentDelta("0.007500000000", "0.009000000000")
	if err != nil || percent == nil || *percent != 20 {
		t.Fatalf("unexpected cost percent: %v %v", percent, err)
	}
}

func TestUnpricedTokensKeepCostUnknown(t *testing.T) {
	input, output, total, err := costsForTokens(nil, 1000, 500)
	if err != nil || input != nil || output != nil || total != nil {
		t.Fatalf("unpriced cost must be nil: %v %v %v %v", input, output, total, err)
	}
}
