import type { TopicRecord } from "./types.ts"

export interface TopicNode {
  name: string
  path: string
  subscribers: number | null
  retained: boolean
  children: TopicNode[]
}

export function buildTopicTree(topics: TopicRecord[]): TopicNode[] {
  const roots: TopicNode[] = []
  const sorted = [...topics].sort((left, right) => left.topic.localeCompare(right.topic))
  for (const topic of sorted) {
    const levels = topic.topic.split("/")
    let siblings = roots
    let path = ""
    levels.forEach((name, index) => {
      path = path.length === 0 ? name : `${path}/${name}`
      let node = siblings.find((item) => item.name === name && item.path === path)
      if (!node) {
        node = {
          name,
          path,
          subscribers: null,
          retained: false,
          children: [],
        }
        siblings.push(node)
      }
      if (index === levels.length - 1) {
        node.subscribers = topic.subscribers
        node.retained = topic.retained
      }
      siblings = node.children
    })
  }
  return roots
}
