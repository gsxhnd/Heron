import { create } from "zustand"

import type { ClientNotice, Sample, Snapshot } from "../lib/types.ts"

const MAX_SAMPLES = 60
const MAX_NOTICES = 20

interface LiveState {
  connected: boolean
  snapshot: Snapshot | null
  samples: Sample[]
  notices: ClientNotice[]
  setConnected: (connected: boolean) => void
  applySnapshot: (snapshot: Snapshot) => void
  pushNotice: (notice: ClientNotice) => void
}

export const useLive = create<LiveState>((set) => ({
  connected: false,
  snapshot: null,
  samples: [],
  notices: [],
  setConnected: (connected) => set({ connected }),
  applySnapshot: (snapshot) =>
    set((state) => {
      const sample: Sample = {
        at: Date.now(),
        connections: snapshot.connections,
        receivePerSec: snapshot.receive_per_sec,
        sendPerSec: snapshot.send_per_sec,
      }
      const samples = [...state.samples, sample].slice(-MAX_SAMPLES)
      return { snapshot, samples }
    }),
  pushNotice: (notice) =>
    set((state) => ({
      notices: [notice, ...state.notices].slice(0, MAX_NOTICES),
    })),
}))
