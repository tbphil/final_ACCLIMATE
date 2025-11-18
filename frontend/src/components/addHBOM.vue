<template>
  <!-- Backdrop & Modal -->
  <div v-if="visible" class="modal-backdrop fade show"></div>
  <div v-if="visible" class="modal fade show" tabindex="-1" style="display:block;">
    <div class="modal-dialog modal-xl">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title">HBOM Upload & Preview</h5>
          <button type="button" class="btn-close" @click="close"></button>
        </div>

        <div class="modal-body">
          <!-- STEP 1: Upload -->
          <div v-if="step==='upload'" class="text-center">
            <input ref="fileInput" type="file" @change="onFileChange" class="form-control mb-3" />
            <select v-model="sector" class="form-select mb-3">
              <option disabled value="">Select sector…</option>
              <option v-for="s in sectors" :key="s">{{ s }}</option>
            </select>
            <button :disabled="!file || !sector" @click="onPreview" class="btn btn-primary">
              Preview
            </button>
          </div>

          <!-- STEP 2: Preview -->
          <div v-else-if="step==='preview'">
            <!-- Workbook duplicates header with actions -->
            <div class="d-flex justify-content-between align-items-center">
              <h6 class="mb-0">Workbook duplicates</h6>
              <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-secondary"
                        @click="uncheckAll"
                        :disabled="!reviewItems.length">
                  Uncheck all
                </button>
                <button class="btn btn-outline-secondary"
                        @click="checkAll"
                        :disabled="!reviewItems.length">
                  Check all
                </button>
              </div>
            </div>

            <!-- Duplicates list -->
            <ul v-if="reviewItems.length" class="list-group mb-3 mt-2">
              <li v-for="item in reviewItems"
                  :key="item.key"
                  class="list-group-item d-flex align-items-center">
                <input
                  type="checkbox"
                  :checked="item.isDuplicate"
                  @change="toggleDup(item, $event)"
                  class="form-check-input me-2"
                />
                <div class="flex-grow-1">
                  <div><strong>Incoming:</strong> {{ item.incomingPath }}</div>
                  <div><strong>Existing:</strong> {{ item.existingPath }}</div>
                </div>
                <span class="badge ms-auto bg-secondary">{{ item.similarity }}%</span>
              </li>
            </ul>
            <p v-else class="text-success mt-2">No duplicates inside the workbook.</p>

            <hr />

            <!-- Catalogue match -->
            <fieldset class="mb-3">
              <legend>Catalogue match</legend>
              <div v-if="catalogMatches.length">
                <div v-for="c in catalogMatches" :key="c.uuid" class="form-check">
                  <input
                    class="form-check-input"
                    type="radio"
                    name="catalog"
                    :value="c.uuid"
                    v-model="chosenRoot"
                    :id="'cat-'+c.uuid"
                  />
                  <label class="form-check-label" :for="'cat-'+c.uuid">
                    Re-use «{{ c.label }}»
                  </label>
                </div>
              </div>
              <div class="form-check">
                <input
                  class="form-check-input"
                  type="radio"
                  name="catalog"
                  value=""
                  v-model="chosenRoot"
                  id="cat-new"
                />
                <label class="form-check-label" for="cat-new">
                  Create as NEW component
                </label>
              </div>
            </fieldset>

            <!-- Diff preview -->
            <h6>Diff</h6>
            <pre class="border p-2 bg-light small" style="max-height:220px;overflow:auto">
              {{ formattedDiff }}
            </pre>

            <!-- Actions -->
            <button @click="onCommit" class="btn btn-success">Commit</button>
            <button @click="close" class="btn btn-secondary ms-2">Cancel</button>
          </div>

          <!-- STEP 3: Done -->
          <div v-else class="text-center">
            <p>HBOM uploaded successfully!</p>
            <button @click="close" class="btn btn-primary">Close</button>
          </div>
        </div>

      </div>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, ref, computed } from 'vue'

/* v-model wires */
const props   = defineProps({ modelValue: Boolean })
const emit    = defineEmits(['update:modelValue'])
const visible = computed({
  get: () => props.modelValue,
  set: v  => emit('update:modelValue', v)
})
function close() { 
  resetForm()
  visible.value = false }

/* state */
const step        = ref('upload')     // upload | preview | done
const file        = ref(null)
const sector      = ref('')
const sectors     = ['Energy Grid', 'Transportation', 'Water']
const previewResp = ref(null)
const chosenRoot  = ref('')
const fileInput = ref(null)

// track which duplicates are checked (by incomingUuid)
const checkedDupes = ref(new Set())

/* computed */
const formattedDiff = computed(() => {
  if (!previewResp.value?.diff) return '(no diff)'
  return JSON.stringify(previewResp.value.diff, null, 2)
})
const catalogMatches = computed(() => previewResp.value?.catalogMatches || [])

// Build the items for the UI from backend duplicates
const reviewItems = computed(() => {
  const raw = previewResp.value?.duplicates || []
  return raw.map(d => {
    const inc = d.incomingUuid // required for mapping inc→use
    return {
      key:           `dup-${inc}-${d.originalUuid}`,
      incUuid:       inc,
      incomingPath:  d.incomingPath,
      existingPath:  d.existingPath,
      similarity:    d.similarity,
      uuid:          d.originalUuid,           // existing node to re-use
      isDuplicate:   checkedDupes.value.has(inc),
    }
  })
})

/* handlers */
function onFileChange (e) {
  file.value = e.target.files[0]
}

async function onPreview () {
  const fd = new FormData()
  fd.append('file', file.value)
  fd.append('sector', sector.value)

  const res = await fetch('http://127.0.0.1:8000/api/hbom/preview', {
    method: 'POST',
    body: fd
  }).then(r => r.json())

  previewResp.value = res

  // initialize: all duplicates start checked
  const all = (res.duplicates || []).map(d => d.incomingUuid)
  checkedDupes.value = new Set(all)

  step.value = 'preview'
}

function toggleDup(item, ev) {
  const next = new Set(checkedDupes.value)
  if (ev.target.checked) next.add(item.incUuid)
  else next.delete(item.incUuid)
  checkedDupes.value = next
}

function uncheckAll() {
  checkedDupes.value = new Set()
}

function checkAll() {
  const raw = previewResp.value?.duplicates || []
  checkedDupes.value = new Set(raw.map(d => d.incomingUuid))
}

async function onCommit () {
  const fd = new FormData()
  fd.append('file', file.value)
  fd.append('sector', sector.value)

  // Build { root, duplicates:[{inc, use}] } from the Set
  const raw = previewResp.value?.duplicates || []
  const reuse = raw
    .filter(d => checkedDupes.value.has(d.incomingUuid))
    .map(d => ({ inc: d.incomingUuid, use: d.originalUuid }))

  fd.append('decisions', JSON.stringify({
    root: chosenRoot.value || '',
    duplicates: reuse
  }))

  // POST (no unused variable to appease ESLint)
  await fetch('http://127.0.0.1:8000/api/hbom/commit', {
    method: 'POST',
    body: fd
  })

  step.value = 'done'
}

function resetForm () {
    step.value = 'upload'
    file.value = null
    sector.value = ''
    previewResp.value = ''
    chosenRoot.value = ''
    checkedDupes.value = new Set()

    if (fileInput.value) fileInput.value.value = ''
}
</script>

<style scoped>
.modal-backdrop { z-index: 1040; }
.modal { z-index: 1050; }
</style>