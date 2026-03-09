"""Tests for the YAML config parser (provider, query, fields, contracts, steps, writer, table_aliases)."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from tiders_core.ingest import ProviderKind, QueryKind
from tiders_core.ingest import svm
from tiders_core.svm_decode import Array, FixedArray, Option, Struct, Enum

from tiders.config import (
    Base58EncodeConfig,
    CastByTypeConfig,
    CastConfig,
    DataFusionStepConfig,
    DeltaLakeWriterConfig,
    DuckdbWriterConfig,
    EvmDecodeEventsConfig,
    EvmTableAliases,
    GlaciersEventsConfig,
    HexEncodeConfig,
    PolarsStepConfig,
    PyArrowDatasetWriterConfig,
    SetChainIdConfig,
    StepKind,
    SvmDecodeInstructionsConfig,
    SvmDecodeLogsConfig,
    SvmTableAliases,
    U256ToBinaryConfig,
    WriterKind,
)
from tiders.cli.tiders_yaml_parser import (
    ContractInfo,
    YamlConfigError,
    parse_contracts,
    parse_pa_type,
    parse_provider,
    parse_query,
    parse_steps,
    parse_table_aliases,
    parse_tiders_yaml,
    parse_writer,
    resolve_contract_refs,
    _parse_dyntype,
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
MINIMAL_ABI = json.dumps(
    [
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
    ]
)


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
        result = parse_contracts(contracts_yaml, tmp_path)
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
        result = parse_contracts(contracts_yaml, tmp_path)
        assert "Transfer" in result["Token"].events

    def test_missing_name_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required key 'name'"):
            parse_contracts([{"details": [{"address": "0x1"}]}], tmp_path)

    def test_missing_details_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing or empty 'details'"):
            parse_contracts([{"name": "Foo"}], tmp_path)

    def test_missing_abi_file_raises(self, tmp_path):
        contracts_yaml = [
            {
                "name": "Bad",
                "details": [{"address": "0x1", "abi": "nonexistent.json"}],
            }
        ]
        with pytest.raises(YamlConfigError, match="ABI file not found"):
            parse_contracts(contracts_yaml, tmp_path)

    def test_contract_without_abi(self, tmp_path):
        """Contract with just an address (no ABI) should work."""
        contracts_yaml = [{"name": "Simple", "details": [{"address": "0xdeadbeef"}]}]
        result = parse_contracts(contracts_yaml, tmp_path)
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
        result = resolve_contract_refs("MyToken.Events.Transfer.topic0", contracts)
        assert result == "0x" + "aa" * 32

    def test_resolve_event_signature(self, contracts):
        result = resolve_contract_refs("MyToken.Events.Transfer.signature", contracts)
        assert "Transfer" in result

    def test_resolve_function_selector(self, contracts):
        result = resolve_contract_refs("MyToken.Functions.approve.selector", contracts)
        assert result == "0x095ea7b3"

    def test_resolve_nested_dict(self, contracts):
        data = {
            "logs": [
                {
                    "address": "MyToken.address",
                    "topic0": "MyToken.Events.Transfer.topic0",
                }
            ]
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
# Integration: parse_tiders_yaml
# ---------------------------------------------------------------------------


class TestParseTidersYaml:
    def _duckdb_writer(self, tmp_path):
        return {"kind": "duckdb", "config": {"path": str(tmp_path / "test.duckdb")}}

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
            "writer": self._duckdb_writer(tmp_path),
        }
        provider, query, steps, writer, table_aliases, contracts = parse_tiders_yaml(
            raw_config, tmp_path
        )

        assert provider.kind == ProviderKind.HYPERSYNC
        assert provider.url == "https://eth.hypersync.xyz"

        assert query.kind == QueryKind.EVM
        assert query.params.from_block == 100
        assert query.params.logs[0].address == ["0xabc"]
        # topic0 should be resolved from ABI
        assert query.params.logs[0].topic0[0].startswith("0x")
        assert query.params.fields.log.address is True

        assert "Token" in contracts

    def test_no_contracts_section(self, tmp_path):
        raw_config = {
            "provider": {"kind": "sqd"},
            "query": {"kind": "evm", "from_block": 0},
            "writer": self._duckdb_writer(tmp_path),
        }
        provider, query, steps, writer, table_aliases, contracts = parse_tiders_yaml(
            raw_config, tmp_path
        )
        assert provider.kind == ProviderKind.SQD
        assert query.kind == QueryKind.EVM
        assert contracts == {}

    def test_missing_provider_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required 'provider'"):
            parse_tiders_yaml(
                {"query": {"kind": "evm"}, "writer": self._duckdb_writer(tmp_path)},
                tmp_path,
            )

    def test_missing_query_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required 'query'"):
            parse_tiders_yaml(
                {
                    "provider": {"kind": "hypersync"},
                    "writer": self._duckdb_writer(tmp_path),
                },
                tmp_path,
            )

    def test_missing_writer_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required 'writer'"):
            parse_tiders_yaml(
                {
                    "provider": {"kind": "sqd"},
                    "query": {"kind": "evm", "from_block": 0},
                },
                tmp_path,
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
            "writer": self._duckdb_writer(tmp_path),
        }
        provider, query, steps, writer, table_aliases, contracts = parse_tiders_yaml(
            raw_config, tmp_path
        )
        assert provider.kind == ProviderKind.YELLOWSTONE_GRPC
        assert query.kind == QueryKind.SVM
        assert query.params.from_block == 200_000_000
        assert query.params.fields.instruction.data is True


# ---------------------------------------------------------------------------
# PyArrow type string parsing
# ---------------------------------------------------------------------------


class TestParsePaType:
    def test_simple_types(self):
        assert parse_pa_type("int32", "") == pa.int32()
        assert parse_pa_type("float64", "") == pa.float64()
        assert parse_pa_type("string", "") == pa.string()
        assert parse_pa_type("binary", "") == pa.binary()
        assert parse_pa_type("bool", "") == pa.bool_()
        assert parse_pa_type("uint64", "") == pa.uint64()

    def test_decimal128(self):
        result = parse_pa_type("decimal128(38,0)", "")
        assert result == pa.decimal128(38, 0)

    def test_decimal128_with_scale(self):
        result = parse_pa_type("decimal128(18,6)", "")
        assert result == pa.decimal128(18, 6)

    def test_decimal256(self):
        result = parse_pa_type("decimal256(76,0)", "")
        assert result == pa.decimal256(76, 0)

    def test_unknown_type_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown type 'foobar'"):
            parse_pa_type("foobar", "test.path")

    def test_bad_decimal_params_raises(self):
        with pytest.raises(YamlConfigError, match="Invalid decimal128"):
            parse_pa_type("decimal128(abc)", "test")

    def test_whitespace_stripped(self):
        result = parse_pa_type("  int32  ", "")
        assert result == pa.int32()


# ---------------------------------------------------------------------------
# DynType parsing
# ---------------------------------------------------------------------------


class TestParseDynType:
    def test_primitives(self):
        assert _parse_dyntype("u64", "") == "u64"
        assert _parse_dyntype("bool", "") == "bool"
        assert _parse_dyntype("i128", "") == "i128"

    def test_unknown_primitive_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown DynType primitive"):
            _parse_dyntype("string", "test")

    def test_array(self):
        result = _parse_dyntype({"type": "array", "element": "u8"}, "")
        assert isinstance(result, Array)
        assert result.element_type == "u8"

    def test_fixed_array(self):
        result = _parse_dyntype(
            {"type": "fixed_array", "element": "u8", "size": 32}, ""
        )
        assert isinstance(result, FixedArray)
        assert result.element_type == "u8"
        assert result.size == 32

    def test_option(self):
        result = _parse_dyntype({"type": "option", "element": "u64"}, "")
        assert isinstance(result, Option)
        assert result.element_type == "u64"

    def test_struct(self):
        result = _parse_dyntype(
            {
                "type": "struct",
                "fields": [
                    {"name": "x", "type": "u64"},
                    {"name": "y", "type": "i32"},
                ],
            },
            "",
        )
        assert isinstance(result, Struct)
        assert len(result.fields) == 2
        assert result.fields[0].name == "x"
        assert result.fields[0].element_type == "u64"

    def test_enum(self):
        result = _parse_dyntype(
            {
                "type": "enum",
                "variants": [
                    {"name": "None"},
                    {"name": "Some", "type": "u64"},
                ],
            },
            "",
        )
        assert isinstance(result, Enum)
        assert len(result.variants) == 2
        assert result.variants[0].name == "None"
        assert result.variants[0].element_type is None
        assert result.variants[1].name == "Some"
        assert result.variants[1].element_type == "u64"

    def test_nested_complex(self):
        """Array of structs."""
        result = _parse_dyntype(
            {
                "type": "array",
                "element": {
                    "type": "struct",
                    "fields": [{"name": "val", "type": "u128"}],
                },
            },
            "",
        )
        assert isinstance(result, Array)
        assert isinstance(result.element_type, Struct)

    def test_missing_type_key_raises(self):
        with pytest.raises(YamlConfigError, match="must have a 'type' key"):
            _parse_dyntype({"element": "u8"}, "")

    def test_array_missing_element_raises(self):
        with pytest.raises(YamlConfigError, match="'element' key"):
            _parse_dyntype({"type": "array"}, "")

    def test_fixed_array_missing_size_raises(self):
        with pytest.raises(YamlConfigError, match="'size' key"):
            _parse_dyntype({"type": "fixed_array", "element": "u8"}, "")

    def test_struct_missing_fields_raises(self):
        with pytest.raises(YamlConfigError, match="'fields' list"):
            _parse_dyntype({"type": "struct"}, "")

    def test_enum_missing_variants_raises(self):
        with pytest.raises(YamlConfigError, match="'variants' list"):
            _parse_dyntype({"type": "enum"}, "")

    def test_unknown_complex_type_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown complex DynType"):
            _parse_dyntype({"type": "map"}, "")


# ---------------------------------------------------------------------------
# Steps parsing
# ---------------------------------------------------------------------------


class TestParseSteps:
    def test_evm_decode_events(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "evm_decode_events",
                    "config": {
                        "event_signature": "Transfer(address indexed,address indexed,uint256)",
                        "input_table": "logs",
                        "output_table": "transfers",
                    },
                }
            ],
            tmp_path,
        )
        assert len(steps) == 1
        s = steps[0]
        assert s.kind == StepKind.EVM_DECODE_EVENTS
        assert isinstance(s.config, EvmDecodeEventsConfig)
        assert (
            s.config.event_signature
            == "Transfer(address indexed,address indexed,uint256)"
        )
        assert s.config.input_table == "logs"
        assert s.config.output_table == "transfers"
        assert s.config.hstack is True

    def test_evm_decode_events_missing_signature_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="event_signature"):
            parse_steps([{"kind": "evm_decode_events", "config": {}}], tmp_path)

    def test_cast_by_type(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "cast_by_type",
                    "config": {
                        "from_type": "decimal256(76,0)",
                        "to_type": "decimal128(38,0)",
                    },
                }
            ],
            tmp_path,
        )
        s = steps[0]
        assert s.kind == StepKind.CAST_BY_TYPE
        assert isinstance(s.config, CastByTypeConfig)
        assert s.config.from_type == pa.decimal256(76, 0)
        assert s.config.to_type == pa.decimal128(38, 0)

    def test_cast_by_type_missing_from_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="from_type"):
            parse_steps(
                [{"kind": "cast_by_type", "config": {"to_type": "int32"}}],
                tmp_path,
            )

    def test_cast(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "cast",
                    "config": {
                        "table_name": "transfers",
                        "mappings": {
                            "amount": "decimal128(38,0)",
                            "block_number": "int64",
                        },
                    },
                }
            ],
            tmp_path,
        )
        s = steps[0]
        assert s.kind == StepKind.CAST
        assert isinstance(s.config, CastConfig)
        assert s.config.table_name == "transfers"
        assert s.config.mappings["amount"] == pa.decimal128(38, 0)
        assert s.config.mappings["block_number"] == pa.int64()

    def test_cast_missing_table_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="table_name"):
            parse_steps(
                [{"kind": "cast", "config": {"mappings": {"a": "int32"}}}],
                tmp_path,
            )

    def test_cast_missing_mappings_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="mappings"):
            parse_steps(
                [{"kind": "cast", "config": {"table_name": "t"}}],
                tmp_path,
            )

    def test_hex_encode_defaults(self, tmp_path):
        steps = parse_steps([{"kind": "hex_encode"}], tmp_path)
        s = steps[0]
        assert s.kind == StepKind.HEX_ENCODE
        assert isinstance(s.config, HexEncodeConfig)
        assert s.config.tables is None
        assert s.config.prefixed is True

    def test_hex_encode_with_config(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "hex_encode",
                    "config": {"tables": ["logs", "blocks"], "prefixed": False},
                }
            ],
            tmp_path,
        )
        assert steps[0].config.tables == ["logs", "blocks"]
        assert steps[0].config.prefixed is False

    def test_base58_encode(self, tmp_path):
        steps = parse_steps([{"kind": "base58_encode"}], tmp_path)
        assert steps[0].kind == StepKind.BASE58_ENCODE
        assert isinstance(steps[0].config, Base58EncodeConfig)

    def test_u256_to_binary(self, tmp_path):
        steps = parse_steps([{"kind": "u256_to_binary"}], tmp_path)
        assert steps[0].kind == StepKind.U256_TO_BINARY
        assert isinstance(steps[0].config, U256ToBinaryConfig)

    def test_set_chain_id(self, tmp_path):
        steps = parse_steps(
            [{"kind": "set_chain_id", "config": {"chain_id": 1}}], tmp_path
        )
        assert steps[0].kind == StepKind.SET_CHAIN_ID
        assert isinstance(steps[0].config, SetChainIdConfig)
        assert steps[0].config.chain_id == 1

    def test_set_chain_id_missing_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="chain_id"):
            parse_steps([{"kind": "set_chain_id", "config": {}}], tmp_path)

    def test_glaciers_events(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "glaciers_events",
                    "config": {"abi_db_path": "/path/to/db"},
                }
            ],
            tmp_path,
        )
        assert steps[0].kind == StepKind.GLACIERS_EVENTS
        assert isinstance(steps[0].config, GlaciersEventsConfig)
        assert steps[0].config.abi_db_path == "/path/to/db"

    def test_missing_kind_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Missing required key 'kind'"):
            parse_steps([{"config": {}}], tmp_path)

    def test_unknown_kind_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Unknown step kind 'bogus'"):
            parse_steps([{"kind": "bogus"}], tmp_path)

    def test_polars_rejected(self, tmp_path):
        with pytest.raises(YamlConfigError, match="cannot be used directly in YAML"):
            parse_steps([{"kind": "polars"}], tmp_path)

    def test_datafusion_rejected(self, tmp_path):
        with pytest.raises(YamlConfigError, match="cannot be used directly in YAML"):
            parse_steps([{"kind": "datafusion"}], tmp_path)

    def test_step_name(self, tmp_path):
        steps = parse_steps([{"kind": "hex_encode", "name": "my hex step"}], tmp_path)
        assert steps[0].name == "my hex step"

    def test_multiple_steps(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "evm_decode_events",
                    "config": {"event_signature": "Transfer(address,address,uint256)"},
                },
                {"kind": "hex_encode"},
                {
                    "kind": "cast_by_type",
                    "config": {
                        "from_type": "decimal256(76,0)",
                        "to_type": "decimal128(38,0)",
                    },
                },
            ],
            tmp_path,
        )
        assert len(steps) == 3
        assert steps[0].kind == StepKind.EVM_DECODE_EVENTS
        assert steps[1].kind == StepKind.HEX_ENCODE
        assert steps[2].kind == StepKind.CAST_BY_TYPE

    def test_steps_not_list_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="must be a list"):
            parse_steps("not a list", tmp_path)


# ---------------------------------------------------------------------------
# SVM decode steps
# ---------------------------------------------------------------------------


class TestParseSvmDecodeSteps:
    def test_svm_decode_instructions(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "svm_decode_instructions",
                    "config": {
                        "instruction_signature": {
                            "discriminator": "e445a52e51cb9a1d",
                            "params": [
                                {"name": "amount", "type": "u64"},
                                {
                                    "name": "data",
                                    "type": {"type": "array", "element": "u8"},
                                },
                            ],
                            "accounts_names": ["source", "destination", "authority"],
                        },
                        "input_table": "instructions",
                        "output_table": "decoded",
                    },
                }
            ],
            tmp_path,
        )
        s = steps[0]
        assert s.kind == StepKind.SVM_DECODE_INSTRUCTIONS
        assert isinstance(s.config, SvmDecodeInstructionsConfig)
        sig = s.config.instruction_signature
        assert sig.discriminator == "e445a52e51cb9a1d"
        assert len(sig.params) == 2
        assert sig.params[0].name == "amount"
        assert sig.params[0].param_type == "u64"
        assert isinstance(sig.params[1].param_type, Array)
        assert sig.accounts_names == ["source", "destination", "authority"]

    def test_svm_decode_instructions_missing_sig_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="instruction_signature"):
            parse_steps([{"kind": "svm_decode_instructions", "config": {}}], tmp_path)

    def test_svm_decode_logs(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "svm_decode_logs",
                    "config": {
                        "log_signature": {
                            "params": [
                                {"name": "amount", "type": "u64"},
                            ],
                        },
                    },
                }
            ],
            tmp_path,
        )
        s = steps[0]
        assert s.kind == StepKind.SVM_DECODE_LOGS
        assert isinstance(s.config, SvmDecodeLogsConfig)
        assert len(s.config.log_signature.params) == 1

    def test_svm_decode_logs_missing_sig_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="log_signature"):
            parse_steps([{"kind": "svm_decode_logs", "config": {}}], tmp_path)

    def test_svm_struct_params(self, tmp_path):
        """Test parsing a complex struct type in instruction params."""
        steps = parse_steps(
            [
                {
                    "kind": "svm_decode_instructions",
                    "config": {
                        "instruction_signature": {
                            "discriminator": "abcd",
                            "params": [
                                {
                                    "name": "data",
                                    "type": {
                                        "type": "struct",
                                        "fields": [
                                            {"name": "x", "type": "u64"},
                                            {
                                                "name": "y",
                                                "type": {
                                                    "type": "option",
                                                    "element": "u32",
                                                },
                                            },
                                        ],
                                    },
                                },
                            ],
                            "accounts_names": [],
                        },
                    },
                }
            ],
            tmp_path,
        )
        param = steps[0].config.instruction_signature.params[0]
        assert isinstance(param.param_type, Struct)
        assert len(param.param_type.fields) == 2
        assert isinstance(param.param_type.fields[1].element_type, Option)


# ---------------------------------------------------------------------------
# SQL step
# ---------------------------------------------------------------------------


class TestSqlStep:
    def test_sql_step(self, tmp_path):
        steps = parse_steps(
            [
                {
                    "kind": "sql",
                    "config": {
                        "queries": [
                            "CREATE TABLE result AS SELECT * FROM logs WHERE block_number > 100",
                        ],
                    },
                }
            ],
            tmp_path,
        )
        s = steps[0]
        assert s.kind == StepKind.DATAFUSION
        assert isinstance(s.config, DataFusionStepConfig)
        assert s.name == "sql"

    def test_sql_step_single_query_string(self, tmp_path):
        """A single query string (not list) should also work."""
        steps = parse_steps(
            [
                {
                    "kind": "sql",
                    "config": {"queries": "SELECT 1"},
                }
            ],
            tmp_path,
        )
        assert steps[0].kind == StepKind.DATAFUSION

    def test_sql_step_missing_queries_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="queries"):
            parse_steps([{"kind": "sql", "config": {}}], tmp_path)


# ---------------------------------------------------------------------------
# python_file step
# ---------------------------------------------------------------------------


class TestPythonFileStep:
    def test_python_file_datafusion(self, tmp_path):
        py_file = tmp_path / "my_transform.py"
        py_file.write_text("def my_runner(ctx, tables, context):\n    return tables\n")
        steps = parse_steps(
            [
                {
                    "kind": "python_file",
                    "config": {
                        "file": str(py_file),
                        "function": "my_runner",
                    },
                }
            ],
            tmp_path,
        )
        s = steps[0]
        assert s.kind == StepKind.DATAFUSION
        assert isinstance(s.config, DataFusionStepConfig)
        assert callable(s.config.runner)

    def test_python_file_polars(self, tmp_path):
        py_file = tmp_path / "polars_step.py"
        py_file.write_text("def transform(tables, context):\n    return tables\n")
        steps = parse_steps(
            [
                {
                    "kind": "python_file",
                    "config": {
                        "file": str(py_file),
                        "function": "transform",
                        "step_type": "polars",
                    },
                }
            ],
            tmp_path,
        )
        s = steps[0]
        assert s.kind == StepKind.POLARS
        assert isinstance(s.config, PolarsStepConfig)

    def test_python_file_with_context(self, tmp_path):
        py_file = tmp_path / "ctx_step.py"
        py_file.write_text("def run(ctx, tables, context):\n    return tables\n")
        steps = parse_steps(
            [
                {
                    "kind": "python_file",
                    "config": {
                        "file": str(py_file),
                        "function": "run",
                        "context": {"key": "value"},
                    },
                }
            ],
            tmp_path,
        )
        assert steps[0].config.context == {"key": "value"}

    def test_python_file_missing_file_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="'file' key"):
            parse_steps(
                [{"kind": "python_file", "config": {"function": "f"}}],
                tmp_path,
            )

    def test_python_file_missing_function_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="'function' key"):
            parse_steps(
                [{"kind": "python_file", "config": {"file": "x.py"}}],
                tmp_path,
            )

    def test_python_file_not_found_raises(self, tmp_path):
        with pytest.raises(YamlConfigError, match="Python file not found"):
            parse_steps(
                [
                    {
                        "kind": "python_file",
                        "config": {
                            "file": "nonexistent.py",
                            "function": "f",
                        },
                    }
                ],
                tmp_path,
            )

    def test_python_file_function_not_found_raises(self, tmp_path):
        py_file = tmp_path / "empty.py"
        py_file.write_text("x = 1\n")
        with pytest.raises(YamlConfigError, match="Function 'missing' not found"):
            parse_steps(
                [
                    {
                        "kind": "python_file",
                        "config": {
                            "file": str(py_file),
                            "function": "missing",
                        },
                    }
                ],
                tmp_path,
            )

    def test_python_file_bad_step_type_raises(self, tmp_path):
        py_file = tmp_path / "step.py"
        py_file.write_text("def f(ctx, t, c): return t\n")
        with pytest.raises(YamlConfigError, match="Unknown step_type"):
            parse_steps(
                [
                    {
                        "kind": "python_file",
                        "config": {
                            "file": str(py_file),
                            "function": "f",
                            "step_type": "spark",
                        },
                    }
                ],
                tmp_path,
            )

    def test_python_file_relative_path(self, tmp_path):
        py_file = tmp_path / "transforms" / "my_step.py"
        py_file.parent.mkdir()
        py_file.write_text("def run(ctx, t, c): return t\n")
        steps = parse_steps(
            [
                {
                    "kind": "python_file",
                    "config": {
                        "file": "transforms/my_step.py",
                        "function": "run",
                    },
                }
            ],
            tmp_path,
        )
        assert steps[0].kind == StepKind.DATAFUSION


# ---------------------------------------------------------------------------
# Writer parsing
# ---------------------------------------------------------------------------


class TestParseWriter:
    def test_missing_kind_raises(self):
        with pytest.raises(YamlConfigError, match="Missing required key 'kind'"):
            parse_writer({"config": {}})

    def test_invalid_kind_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown writer kind 'nosql'"):
            parse_writer({"kind": "nosql", "config": {}})

    def test_writer_must_be_dict(self):
        with pytest.raises(YamlConfigError, match="'writer' must be a mapping"):
            parse_writer("not_a_dict")

    def test_writer_config_must_be_dict(self):
        with pytest.raises(YamlConfigError, match="'writer.config' must be a mapping"):
            parse_writer({"kind": "duckdb", "config": "bad"})

    # -- DuckDB --

    def test_duckdb_basic(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        result = parse_writer({"kind": "duckdb", "config": {"path": db_path}})
        assert result.kind == WriterKind.DUCKDB
        assert isinstance(result.config, DuckdbWriterConfig)
        assert result.config.connection is not None

    def test_duckdb_missing_path_raises(self):
        with pytest.raises(
            YamlConfigError, match="DuckDB writer requires 'config.path'"
        ):
            parse_writer({"kind": "duckdb", "config": {}})

    def test_duckdb_path_must_be_string(self):
        with pytest.raises(YamlConfigError, match="'config.path' must be a string"):
            parse_writer({"kind": "duckdb", "config": {"path": 123}})

    # -- Delta Lake --

    def test_delta_lake_basic(self):
        result = parse_writer(
            {
                "kind": "delta_lake",
                "config": {"data_uri": "s3://bucket/delta"},
            }
        )
        assert result.kind == WriterKind.DELTA_LAKE
        assert isinstance(result.config, DeltaLakeWriterConfig)
        assert result.config.data_uri == "s3://bucket/delta"

    def test_delta_lake_with_options(self):
        result = parse_writer(
            {
                "kind": "delta_lake",
                "config": {
                    "data_uri": "/data/delta",
                    "partition_by": {"logs": ["block_number"]},
                    "storage_options": {"key": "val"},
                    "anchor_table": "logs",
                },
            }
        )
        assert result.config.partition_by == {"logs": ["block_number"]}
        assert result.config.storage_options == {"key": "val"}
        assert result.config.anchor_table == "logs"

    def test_delta_lake_missing_data_uri_raises(self):
        with pytest.raises(
            YamlConfigError, match="Delta Lake writer requires 'config.data_uri'"
        ):
            parse_writer({"kind": "delta_lake", "config": {}})

    # -- PyArrow Dataset --

    def test_pyarrow_dataset_basic(self):
        result = parse_writer(
            {
                "kind": "pyarrow_dataset",
                "config": {"base_dir": "/data/output"},
            }
        )
        assert result.kind == WriterKind.PYARROW_DATASET
        assert isinstance(result.config, PyArrowDatasetWriterConfig)
        assert result.config.base_dir == "/data/output"

    def test_pyarrow_dataset_with_options(self):
        result = parse_writer(
            {
                "kind": "pyarrow_dataset",
                "config": {
                    "base_dir": "/data/output",
                    "basename_template": "part-{i}.parquet",
                    "use_threads": False,
                    "max_partitions": 512,
                    "max_open_files": 256,
                    "max_rows_per_file": 1000000,
                    "min_rows_per_group": 100,
                    "max_rows_per_group": 500000,
                    "create_dir": False,
                    "anchor_table": "events",
                    "partitioning": {"events": ["date"]},
                    "partitioning_flavor": {"events": "hive"},
                },
            }
        )
        cfg = result.config
        assert cfg.basename_template == "part-{i}.parquet"
        assert cfg.use_threads is False
        assert cfg.max_partitions == 512
        assert cfg.max_open_files == 256
        assert cfg.max_rows_per_file == 1000000
        assert cfg.min_rows_per_group == 100
        assert cfg.max_rows_per_group == 500000
        assert cfg.create_dir is False
        assert cfg.anchor_table == "events"
        assert cfg.partitioning == {"events": ["date"]}
        assert cfg.partitioning_flavor == {"events": "hive"}

    def test_pyarrow_dataset_missing_base_dir_raises(self):
        with pytest.raises(
            YamlConfigError, match="PyArrow dataset writer requires 'config.base_dir'"
        ):
            parse_writer({"kind": "pyarrow_dataset", "config": {}})

    # -- Default empty config --

    def test_duckdb_default_config_key(self):
        """When 'config' key is omitted, it defaults to empty dict and raises."""
        with pytest.raises(
            YamlConfigError, match="DuckDB writer requires 'config.path'"
        ):
            parse_writer({"kind": "duckdb"})


# ---------------------------------------------------------------------------
# Table aliases parsing
# ---------------------------------------------------------------------------


class TestParseTableAliases:
    def test_evm_basic(self):
        result = parse_table_aliases({"blocks": "my_blocks", "logs": "my_logs"}, "evm")
        assert isinstance(result, EvmTableAliases)
        assert result.blocks == "my_blocks"
        assert result.logs == "my_logs"
        assert result.transactions is None
        assert result.traces is None

    def test_evm_all_fields(self):
        result = parse_table_aliases(
            {"blocks": "b", "transactions": "t", "logs": "l", "traces": "tr"},
            "evm",
        )
        assert result.blocks == "b"
        assert result.transactions == "t"
        assert result.logs == "l"
        assert result.traces == "tr"

    def test_svm_basic(self):
        result = parse_table_aliases(
            {"blocks": "my_blocks", "instructions": "my_ixs"}, "svm"
        )
        assert isinstance(result, SvmTableAliases)
        assert result.blocks == "my_blocks"
        assert result.instructions == "my_ixs"
        assert result.transactions is None

    def test_svm_all_fields(self):
        result = parse_table_aliases(
            {
                "blocks": "b",
                "transactions": "t",
                "instructions": "i",
                "logs": "l",
                "balances": "bal",
                "token_balances": "tb",
                "rewards": "r",
            },
            "svm",
        )
        assert result.blocks == "b"
        assert result.rewards == "r"
        assert result.token_balances == "tb"

    def test_evm_unknown_key_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown EVM table alias keys"):
            parse_table_aliases({"blocks": "b", "instructions": "i"}, "evm")

    def test_svm_unknown_key_raises(self):
        with pytest.raises(YamlConfigError, match="Unknown SVM table alias keys"):
            parse_table_aliases({"traces": "tr"}, "svm")

    def test_unknown_query_kind_raises(self):
        with pytest.raises(YamlConfigError, match="Cannot determine table alias type"):
            parse_table_aliases({"blocks": "b"}, "unknown")

    def test_must_be_dict(self):
        with pytest.raises(YamlConfigError, match="'table_aliases' must be a mapping"):
            parse_table_aliases("not_a_dict", "evm")

    def test_empty_dict_evm(self):
        result = parse_table_aliases({}, "evm")
        assert isinstance(result, EvmTableAliases)
        assert result.blocks is None

    def test_empty_dict_svm(self):
        result = parse_table_aliases({}, "svm")
        assert isinstance(result, SvmTableAliases)
        assert result.blocks is None
