package liteapi

import (
	"context"

	"github.com/tonkeeper/tongo/boc"
	"github.com/tonkeeper/tongo/liteclient"
	"github.com/tonkeeper/tongo/tlb"
	"github.com/tonkeeper/tongo/ton"
	"github.com/tonkeeper/tongo/utils"
)

func (c *Client) RunSmcMethodByIDAtBlock(ctx context.Context, accountID ton.AccountID, methodID int, params tlb.VmStack, block ton.BlockIDExt) (uint32, tlb.VmStack, error) {
	cell := boc.NewCell()
	err := tlb.Marshal(cell, params)
	if err != nil {
		return 0, tlb.VmStack{}, err
	}
	b, err := cell.ToBoc()
	if err != nil {
		return 0, tlb.VmStack{}, err
	}
	client, _, err := c.pool.BestClientByAccountID(ctx, accountID, false)
	if err != nil {
		return 0, tlb.VmStack{}, err
	}
	req := liteclient.LiteServerRunSmcMethodRequest{
		Mode:     4,
		Id:       liteclient.BlockIDExt(block),
		Account:  liteclient.AccountID(accountID),
		MethodId: uint64(methodID),
		Params:   b,
	}
	res, err := client.LiteServerRunSmcMethod(ctx, req)
	if err != nil {
		return 0, tlb.VmStack{}, err
	}
	var result tlb.VmStack
	if res.ExitCode == 4294967040 { //-256
		return res.ExitCode, tlb.VmStack{}, ErrAccountNotFound
	}
	cells, err := boc.DeserializeBoc(res.Result)
	if err != nil {
		return 0, tlb.VmStack{}, err
	}
	if len(cells) != 1 {
		return 0, tlb.VmStack{}, boc.ErrNotSingleRoot
	}
	err = tlb.Unmarshal(cells[0], &result)
	return res.ExitCode, result, err
}

func (c *Client) RunSmcMethodAtBlock(
	ctx context.Context,
	accountID ton.AccountID,
	method string,
	params tlb.VmStack,
	block ton.BlockIDExt,
) (uint32, tlb.VmStack, error) {
	return c.RunSmcMethodByIDAtBlock(ctx, accountID, utils.MethodIdFromName(method), params, block)
}
