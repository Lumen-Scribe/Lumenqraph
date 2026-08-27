export interface Health {
  lag_ledgers?: number;
  lag?: number;
  last_processed_ledger?: number;
  chain_tip_ledger?: number;
  chain_tip?: number;
  events_ingested_total?: number;
  errors_total?: number;
  status?: string;
  network?: string;
  mounts?: Record<string, string>;
}

export interface Contract {
  contract_id: string;
  event_count: number;
  first_seen_ledger: number;
  last_seen_ledger: number;
}

export interface EventRecord {
  ledger: number;
  event_name?: string;
  event_type: string;
  enriched?: unknown;
  decoded_value?: unknown;
  tx_hash: string;
}

export interface Transfer {
  ledger: number;
  from_addr: string | null;
  to_addr: string | null;
  amount: string;
}

export interface ContractState {
  versions: Array<{
    ledger: number;
    storage: unknown;
    captured_at: string;
  }>;
}

export interface ContractData {
  keys: Array<{
    key: unknown;
    value: unknown;
    ledger: number;
    label?: string;
  }>;
}

export interface InterfaceInfo {
  interface?: ContractInterface;
  functions?: ContractFunction[];
  events?: ContractEvent[];
  structs?: ContractStruct[];
  unions?: ContractUnion[];
  enums?: ContractEnum[];
  fetched_at?: string;
  observed_at?: string;
}

export interface ContractInterface {
  functions?: ContractFunction[];
  events?: ContractEvent[];
  structs?: ContractStruct[];
  unions?: ContractUnion[];
  enums?: ContractEnum[];
}

export interface ContractFunction {
  name: string;
  inputs?: Array<{ name: string; type: unknown }>;
  outputs?: unknown[];
  doc?: string;
}

export interface ContractEvent {
  name: string;
  params?: Array<{ name: string; type: unknown; location: string }>;
  data_format: string;
  doc?: string;
}

export interface ContractStruct {
  name: string;
  fields?: Array<{ name: string; type: unknown }>;
}

export interface ContractUnion {
  name: string;
  cases?: Array<{ name: string; types?: unknown[] }>;
}

export interface ContractEnum {
  name: string;
  cases?: Array<[string, number]>;
}

export interface InterfaceHistory {
  versions?: VersionInfo[];
}

export interface VersionInfo {
  version: number;
  wasm_hash?: string;
  previous_wasm_hash?: string;
  observed_at?: string;
  breaking: boolean;
  diff?: VersionDiff;
}

export interface VersionDiff {
  functions?: DiffSection;
  events?: DiffSection;
  types?: DiffSection;
}

export interface DiffSection {
  removed?: string[];
  added?: string[];
  changed?: Array<{ name: string; from: string; to: string }>;
}
