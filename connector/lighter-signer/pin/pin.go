package main

// Exact cross-platform golden-vector pin, built from the SAME lighter-go revision
// (6d453d1) that the shipped lighter-signer-linux-amd64.so was compiled from.
// Sets ExpiredAt explicitly (impossible through the C-shim) to reproduce the
// Phase-0 (Darwin) golden hashes byte-for-byte on Linux/amd64.

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/elliottech/lighter-go/client"
	clienthttp "github.com/elliottech/lighter-go/client/http"
	"github.com/elliottech/lighter-go/types"
)

const (
	KEY   = "11223344556677889900112233445566778899001122334455667788990011223344556677889900"
	CHAIN = uint32(300)
	AKI   = uint8(2)
	ACC   = int64(1)
	URL   = "https://testnet.zklighter.elliot.ai"
)

func mustClient() *client.TxClient {
	c, err := client.NewTxClient(clienthttp.NewClient(URL), KEY, ACC, AKI, CHAIN)
	if err != nil {
		panic(err)
	}
	return c
}

func createOrderHash(c *client.TxClient, expiredAt, nonce int64) string {
	tx := &types.CreateOrderTxReq{
		MarketIndex: 1, ClientOrderIndex: 123456789, BaseAmount: 1000, Price: 6500000,
		IsAsk: 0, Type: 0, TimeInForce: 2, ReduceOnly: 0, TriggerPrice: 0,
		OrderExpiry: 1800000000000,
	}
	ops := &types.TransactOpts{ExpiredAt: expiredAt, Nonce: &nonce, TxAttributes: &types.L2TxAttributes{}}
	txInfo, err := c.GetCreateOrderTransaction(tx, ops)
	if err != nil {
		panic(err)
	}
	h, err := txInfo.Hash(CHAIN)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(h)
}

func cancelOrderHash(c *client.TxClient, expiredAt, nonce int64) string {
	tx := &types.CancelOrderTxReq{MarketIndex: 1, Index: 987654321}
	ops := &types.TransactOpts{ExpiredAt: expiredAt, Nonce: &nonce, TxAttributes: &types.L2TxAttributes{}}
	txInfo, err := c.GetCancelOrderTransaction(tx, ops)
	if err != nil {
		panic(err)
	}
	h, err := txInfo.Hash(CHAIN)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(h)
}

func check(label, got, want string) {
	ok := got == want
	fmt.Printf("%-28s got=%s\n%-28s want=%s  MATCH=%v\n", label, got, "", want, ok)
}

// serve reads "create|cancel <expiredAt> <nonce>" lines and prints the hash per line.
// Used by the .so-vs-reference differential test.
func serve() {
	c := mustClient()
	sc := bufio.NewScanner(os.Stdin)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 3 {
			continue
		}
		ea, _ := strconv.ParseInt(f[1], 10, 64)
		n, _ := strconv.ParseInt(f[2], 10, 64)
		var h string
		if f[0] == "cancel" {
			h = cancelOrderHash(c, ea, n)
		} else {
			h = createOrderHash(c, ea, n)
		}
		fmt.Fprintln(w, h)
		w.Flush()
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		serve()
		return
	}
	c := mustClient()

	// base_vector (golden_vector.json)
	check("create base ea=..437806", createOrderHash(c, 1785428437806, 42),
		"70eea35982e01dfc151534c05cef8eaf161dd0b8e4fd7ff5e84b7bb2efe17a0522027e5ffa6287c9")

	// golden2_out.json golden_vectors (same fields, nonce 42, varied ExpiredAt)
	g2 := []struct {
		ea   int64
		want string
	}{
		{1785428161890, "d7cb6b0ce134ea96fad7f32a1a6ff73f4471c6ae9fafd7bd55a377fbdcde2475cd3b0dbe1faf40be"},
		{1785428161891, "199e83ac65fad5355f774377b58a9f974ea729fb2ed774c810cb249fa5eeb7331fb986b7198c4bea"},
		{1785428161892, "10596150e34a142d921c085e154edf307ff4c5a02ce2fc3e37f5e4431f95bc073340175aa8ef3ddf"},
		{1785428161893, "f97e712c6d58683399ede3f7f7abe450a86dbf13823cb04fb989d836aa9b87a8f9abe94de92dcec1"},
		{1785428161894, "79fa63ac75cc7903cfa1752d6e53b4b2b9395d328b75421d87b086270a91deb19d7bf0039ba18795"},
		{1785428161895, "5cfc8f8095a28e6384dfe05cac78cf17fda32d72c550e058ebb5fe910cf6d052ad6bcdaea3e2210c"},
	}
	for _, v := range g2 {
		check(fmt.Sprintf("create golden2 ea=..%d", v.ea%1000), createOrderHash(c, v.ea, 42), v.want)
	}

	// cancel vector (golden_out.json cancel_order_runs, nonce 43, ExpiredAt ..092765)
	check("cancel ea=..092765 n=43", cancelOrderHash(c, 1785428092765, 43),
		"f3f1de3293e31609b20f34cb5839914c2a6273d9eb9410940d321f9723974ac3a3b58130def8b83c")
}
