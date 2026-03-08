"""Tests for the YAML config parser (provider, query, fields, contracts)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tiders_core.ingest import ProviderConfig, ProviderKind, Query, QueryKind
from tiders_core.ingest import evm, svm

from tiders.cli.tiders_yaml_parser import (
    ContractInfo,
    YamlConfigError,
    load_contracts,
    parse_provider,
    parse_provider_and_query,
    parse_query,
    resolve_contract_refs,
)


# ---------------------------------------------------------------------------
# Provider parsing
# ---------------------------------------------------------------------------

class TestParseProvider:
    def test_basic_hypersync(self):
        raw = {"kind": "hypersync", "url": "https://eth.hypersync.xyz"}
        result = parse_provider(raw)
        assert result.kind == ProviderKind.HYPERSYNC
        assert result.url == "https://eth.hypersync.xyz"

    def test_sqd_provider(self):
        raw = {"kind": "sqd"}
        result = parse_provider(raw)
        assert result.kind == ProviderKind.SQD
        assert result.url is None

    def test_rpc_with_options(self):
        raw = {
            "kind": "rpc",
            "url": "https://eth.rpc.example.com",
            "batch_size": 100,
            "compute_units_per_second": 500,
            "stop_on_head": True,
        }
        result = parse_provider(raw)
        assert result.kind == ProviderKind.RPC
        assert result.batch_size == 100
        assert result.compute_units_per_second == 500
        assert result.stop_on_head is True

    def test_missing_kind_raises(self):
        with pytest.raises(YamlConfigError, match="Missing required key 'kind'"):
            parse_provider({"url": "https://example.com"})

    def test_unknown_kind_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown provider kind 'foobar'"):
            parse_provider({"kind": "foobar"})

    def test_unknown_option_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown provider options"):
            parse_provider({"kind": "hypersync", "bogus_option": 42})


# ---------------------------------------------------------------------------
# EVM query parsing
# ---------------------------------------------------------------------------

class TestParseEvmQuery:
    def test_basic_query(self):
        raw = {
            "kind": "evm",
            "from_block": 1000,
            "to_block": 2000,
        }
        result = parse_query(raw)
        assert result.kind == QueryKind.EVM
        assert result.params.from_block == 1000
        assert result.params.to_block == 2000

    def test_fields_as_list(self):
        raw = {
            "kind": "evm",
            "from_block": 0,
            "fields": {
                "log": ["address", "topic0", "data", "block_number"],
                "block": ["number", "timestamp"],
            },
        }
        result = parse_query(raw)
        log_fields = result.params.fields.log
        assert log_fields.address is True
        assert log_fields.topic0 is True
        assert log_fields.data is True
        assert log_fields.block_number is True
        assert log_fields.topic1 is False  # not requested

        block_fields = result.params.fields.block
        assert block_fields.number is True
        assert block_fields.timestamp is True
        assert block_fields.hash is False

    def test_fields_as_dict(self):
        raw = {
            "kind": "evm",
            "fields": {
                "log": {"address": True, "topic0": True, "data": False},
            },
        }
        result = parse_query(raw)
        log_fields = result.params.fields.log
        assert log_fields.address is True
        assert log_fields.topic0 is True
        assert log_fields.data is False

    def test_unknown_field_raises(self):
        raw = {
            "kind": "evm",
            "fields": {"log": ["address", "nonexistent_field"]},
        }
        with pytest.raises(YamlConfigError, match="Unknown field 'nonexistent_field'"):
            parse_query(raw)

    def test_unknown_field_category_raises(self):
        raw = {
            "kind": "evm",
            "fields": {"bogus": ["address"]},
        }
        with pytest.raises(YamlConfigError, match="Unknown field categories"):
            parse_query(raw)

    def test_log_request_with_lists(self):
        raw = {
            "kind": "evm",
            "logs": [
                {
                    "address": ["0xabc", "0xdef"],
                    "topic0": ["0x" + "a" * 64],
                    "include_blocks": True,
                },
            ],
        }
        result = parse_query(raw)
        assert len(result.params.logs) == 1
        log_req = result.params.logs[0]
        assert log_req.address == ["0xabc", "0xdef"]
        assert log_req.include_blocks is True

    def test_log_request_single_value_wrapped_in_list(self):
        raw = {
            "kind": "evm",
            "logs": [{"address": "0xabc"}],
        }
        result = parse_query(raw)
        assert result.params.logs[0].address == ["0xabc"]

    def test_topic0_from_event_signature(self):
        """topic0 can be an event signature string, auto-resolved to hash."""
        raw = {
            "kind": "evm",
            "logs": [{"topic0": "Transfer(address,address,uint256)"}],
        }
        result = parse_query(raw)
        topic0 = result.params.logs[0].topic0[0]
        assert topic0.startswith("0x")
        assert len(topic0) == 66

    def test_topic0_hex_hash_passthrough(self):
        hex_hash = "0x" + "ab" * 32
        raw = {
            "kind": "evm",
            "logs": [{"topic0": hex_hash}],
        }
        result = parse_query(raw)
        assert result.params.logs[0].topic0 == [hex_hash]

    def test_transaction_request(self):
        raw = {
            "kind": "evm",
            "transactions": [
                {
                    "from_": "0xsender",
                    "to": ["0xrecv1", "0xrecv2"],
                    "include_blocks": True,
                }
            ],
        }
        result = parse_query(raw)
        assert len(result.params.transactions) == 1
        tx = result.params.transactions[0]
        assert tx.from_ == ["0xsender"]
        assert tx.to == ["0xrecv1", "0xrecv2"]
        assert tx.include_blocks is True

    def test_trace_request(self):
        raw = {
            "kind": "evm",
            "traces": [{"call_type": "call", "include_blocks": True}],
        }
        result = parse_query(raw)
        assert len(result.params.traces) == 1
        assert result.params.traces[0].call_type == ["call"]

    def test_unknown_log_key_raises(self):
        raw = {
            "kind": "evm",
            "logs": [{"address": "0xabc", "bad_key": True}],
        }
        with pytest.raises(YamlConfigError, match="Unknown keys"):
            parse_query(raw)

    def test_unknown_query_key_raises(self):
        raw = {"kind": "evm", "magic_option": True}
        with pytest.raises(YamlConfigError, match="Unknown EVM query keys"):
            parse_query(raw)

    def test_missing_kind_raises(self):
        with pytest.raises(YamlConfigError, match="Missing required key 'kind'"):
            parse_query({"from_block": 0})

    def test_unknown_kind_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown query kind"):
            parse_query({"kind": "btc"})


# ---------------------------------------------------------------------------
# SVM query parsing
# ---------------------------------------------------------------------------

class TestParseSvmQuery:
    def test_basic_svm_query(self):
        raw = {
            "kind": "svm",
            "from_block": 100,
            "instructions": [
                {
                    "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                    "include_blocks": True,
                }
            ],
        }
        result = parse_query(raw)
        assert result.kind == QueryKind.SVM
        assert result.params.from_block == 100
        assert len(result.params.instructions) == 1
        assert result.params.instructions[0].program_id == [
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        ]

    def test_svm_fields(self):
        raw = {
            "kind": "svm",
            "fields": {
                "instruction": ["block_slot", "program_id", "data"],
                "block": ["slot", "timestamp"],
            },
        }
        result = parse_query(raw)
        assert result.params.fields.instruction.block_slot is True
        assert result.params.fields.instruction.program_id is True
        assert result.params.fields.instruction.data is True
        assert result.params.fields.block.slot is True

    def test_svm_log_request_with_kind(self):
        raw = {
            "kind": "svm",
            "logs": [{"program_id": "abc", "kind": ["log", "data"]}],
        }
        result = parse_query(raw)
        log_req = result.params.logs[0]
        assert log_req.kind == [svm.LogKind.LOG, svm.LogKind.DATA]

    def test_svm_balance_request(self):
        raw = {
            "kind": "svm",
            "balances": [{"account": "SomeAccount123"}],
        }
        result = parse_query(raw)
        assert result.params.balances[0].account == ["SomeAccount123"]

    def test_svm_token_balance_request(self):
        raw = {
            "kind": "svm",
            "token_balances": [{"account": "AcctXYZ", "include_blocks": True}],
        }
        result = parse_query(raw)
        assert result.params.token_balances[0].account == ["AcctXYZ"]
        assert result.params.token_balances[0].include_blocks is True

    def test_svm_rewards_request(self):
        raw = {
            "kind": "svm",
            "rewards": [{"pubkey": "ValidatorPubkey"}],
        }
        result = parse_query(raw)
        assert result.params.rewards[0].pubkey == ["ValidatorPubkey"]

    def test_unknown_svm_key_raises(self):
        raw = {"kind": "svm", "bogus": True}
        with pytest.raises(YamlConfigError, match="Unknown SVM query keys"):
            parse_query(raw)


# ---------------------------------------------------------------------------
# Contract resolution
# ---------------------------------------------------------------------------

# Minimal ERC20 ABI with Transfer event and approve function
MINIMAL_ABI = json.dumps([
    {
        "type": "event",
        "name": "Transfer",
        "anonymous": False,
        "inputs": [
            {"name": "from", "type": "address", "indexed": True},
            {"name": "to", "type": "address", "indexed": True},
            {"name": "value", "type": "uint256", "indexed": False},
        ],
    },
    {
        "type": "function",
        "name": "approve",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
])


class TestLoadContracts:
    def test_load_contract_with_abi(self, tmp_path):
        abi_file = tmp_path / "erc20.json"
        abi_file.write_text(MINIMAL_ABI)

        contracts_yaml = [
            {
                "name": "MyToken",
                "details": [
                    {
                        "network": "ethereum",
                        "address": "0x1234",
                        "abi": str(abi_file),
                    }
                ],
            }
        ]
        result = load_contracts(contracts_yaml, tmp_path)
        assert "MyToken" in result
        c = result["MyToken"]
        assert c.address == "0x1234"
        assert "Transfer" in c.events
        assert c.events["Transfer"]["topic0"].startswith("0x")
        assert "approve" in c.functions

    def test_load_contract_relative_abi(self, tmp_path):
        abi_file = tmp_path / "abis" / "token.json"
        abi_file.parent.mkdir()
        abi_file.write_text(MINIMAL_ABI)

        contracts_yaml = [
            {
                "name": "Token",
                "details": [{"address": "0xabc", "abi": "abis/token.json"}],
            }
        ]
        result = load_contracts(contracts_yaml, tmp_path)
        assert "Transfer" in result["Token"].events

    def test_missing_name_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required key 'name'"):
            load_contracts([{"details": [{"address": "0x1"}]}], tmp_path)

    def test_missing_details_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing or empty 'details'"):
            load_contracts([{"name": "Foo"}], tmp_path)

    def test_missing_abi_file_raises(self, tmp_path):
        contracts_yaml = [
            {
                "name": "Bad",
                "details": [{"address": "0x1", "abi": "nonexistent.json"}],
            }
        ]
        with pytest.raises(YamlConfigError, match="ABI file not found"):
            load_contracts(contracts_yaml, tmp_path)

    def test_contract_without_abi(self, tmp_path):
        """Contract with just an address (no ABI) should work."""
        contracts_yaml = [
            {"name": "Simple", "details": [{"address": "0xdeadbeef"}]}
        ]
        result = load_contracts(contracts_yaml, tmp_path)
        assert result["Simple"].address == "0xdeadbeef"
        assert result["Simple"].events == {}
        assert result["Simple"].functions == {}


class TestResolveContractRefs:
    @pytest.fixture()
    def contracts(self):
        return {
            "MyToken": ContractInfo(
                name="MyToken",
                address="0x1234abcd",
                events={
                    "Transfer": {
                        "topic0": "0x" + "aa" * 32,
                        "signature": "Transfer(address indexed,address indexed,uint256)",
                    }
                },
                functions={
                    "approve": {
                        "selector": "0x095ea7b3",
                        "signature": "approve(address,uint256)",
                    }
                },
            )
        }

    def test_resolve_address(self, contracts):
        assert resolve_contract_refs("MyToken.address", contracts) == "0x1234abcd"

    def test_resolve_event_topic0(self, contracts):
        result = resolve_contract_refs(
            "MyToken.Events.Transfer.topic0", contracts
        )
        assert result == "0x" + "aa" * 32

    def test_resolve_event_signature(self, contracts):
        result = resolve_contract_refs(
            "MyToken.Events.Transfer.signature", contracts
        )
        assert "Transfer" in result

    def test_resolve_function_selector(self, contracts):
        result = resolve_contract_refs(
            "MyToken.Functions.approve.selector", contracts
        )
        assert result == "0x095ea7b3"

    def test_resolve_nested_dict(self, contracts):
        data = {
            "logs": [{"address": "MyToken.address", "topic0": "MyToken.Events.Transfer.topic0"}]
        }
        result = resolve_contract_refs(data, contracts)
        assert result["logs"][0]["address"] == "0x1234abcd"
        assert result["logs"][0]["topic0"] == "0x" + "aa" * 32

    def test_noncontract_string_passthrough(self, contracts):
        assert resolve_contract_refs("plain string", contracts) == "plain string"
        assert resolve_contract_refs("0xdeadbeef", contracts) == "0xdeadbeef"

    def test_unknown_event_raises(self, contracts):
        with pytest.raises(YamlConfigError, match="Event 'Bogus' not found"):
            resolve_contract_refs("MyToken.Events.Bogus.topic0", contracts)

    def test_unknown_function_raises(self, contracts):
        with pytest.raises(YamlConfigError, match="Function 'Bogus' not found"):
            resolve_contract_refs("MyToken.Functions.Bogus.selector", contracts)

    def test_invalid_ref_pattern_raises(self, contracts):
        with pytest.raises(YamlConfigError, match="Invalid contract reference"):
            resolve_contract_refs("MyToken.bogus_property", contracts)


# ---------------------------------------------------------------------------
# Integration: parse_provider_and_query
# ---------------------------------------------------------------------------

class TestParseProviderAndQuery:
    def test_full_evm_config(self, tmp_path):
        abi_file = tmp_path / "erc20.json"
        abi_file.write_text(MINIMAL_ABI)

        raw_config = {
            "contracts": [
                {
                    "name": "Token",
                    "details": [{"address": "0xabc", "abi": str(abi_file)}],
                }
            ],
            "provider": {"kind": "hypersync", "url": "https://eth.hypersync.xyz"},
            "query": {
                "kind": "evm",
                "from_block": 100,
                "logs": [
                    {
                        "address": "Token.address",
                        "topic0": "Token.Events.Transfer.topic0",
                        "include_blocks": True,
                    }
                ],
                "fields": {
                    "log": ["address", "topic0", "data", "block_number"],
                    "block": ["number", "timestamp"],
                },
            },
        }
        provider, query = parse_provider_and_query(raw_config, tmp_path)

        assert provider.kind == ProviderKind.HYPERSYNC
        assert provider.url == "https://eth.hypersync.xyz"

        assert query.kind == QueryKind.EVM
        assert query.params.from_block == 100
        assert query.params.logs[0].address == ["0xabc"]
        # topic0 should be resolved from ABI
        assert query.params.logs[0].topic0[0].startswith("0x")
        assert query.params.fields.log.address is True

    def test_no_contracts_section(self, tmp_path):
        raw_config = {
            "provider": {"kind": "sqd"},
            "query": {"kind": "evm", "from_block": 0},
        }
        provider, query = parse_provider_and_query(raw_config, tmp_path)
        assert provider.kind == ProviderKind.SQD
        assert query.kind == QueryKind.EVM

    def test_missing_provider_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required 'provider'"):
            parse_provider_and_query({"query": {"kind": "evm"}}, tmp_path)

    def test_missing_query_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required 'query'"):
            parse_provider_and_query(
                {"provider": {"kind": "hypersync"}}, tmp_path
            )

    def test_svm_config(self, tmp_path):
        raw_config = {
            "provider": {
                "kind": "yellowstone_grpc",
                "url": "https://solana.example.com",
            },
            "query": {
                "kind": "svm",
                "from_block": 200_000_000,
                "instructions": [
                    {
                        "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                    }
                ],
                "fields": {
                    "instruction": ["block_slot", "program_id", "data"],
                },
            },
        }
        provider, query = parse_provider_and_query(raw_config, tmp_path)
        assert provider.kind == ProviderKind.YELLOWSTONE_GRPC
        assert query.kind == QueryKind.SVM
        assert query.params.from_block == 200_000_000
        assert query.params.fields.instruction.data is True
